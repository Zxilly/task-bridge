package broker

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
	"github.com/google/uuid"
)

func (b *Broker) requestTaskResume(ctx context.Context, c *ClientPeer, a Args) (model.TaskResumeResult, error) {
	var out model.TaskResumeResult
	err := b.update(ctx, func(tx *storage.Tx) error {
		p := c.Principal
		if err := activePrincipal(tx, p); err != nil {
			return err
		}
		if !b.liveClient(c) {
			return model.Err("stale_connection", "executor connection replaced")
		}
		if !bounded(a.TaskID, 128) || !bounded(a.IdempotencyKey, 200) {
			return model.Err("invalid_arguments", "bounded task_id and idempotency_key are required")
		}
		task, err := getTask(tx, p, a.TaskID)
		if err != nil {
			return err
		}
		if p.Kind != "executor" || task.RequesterID != p.ID || task.ExecutorID != p.ExecutorID || !supportsWorkspace(c, task.WorkspaceID) {
			return model.Err("forbidden", "only the original submitting executor credential may resume this task")
		}
		previous, err := tx.ResumeRequestByKey(p.ID, a.IdempotencyKey)
		if err == nil {
			if previous.TaskID != task.ID {
				return model.Err("idempotency_conflict", "resume key was already used for another task")
			}
			out = model.TaskResumeResult{TaskID: task.ID, ResumeRequest: previous}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if b.draining {
			return model.Err("draining", "server is draining")
		}
		if (task.State != "interrupted" && task.State != "queued") || task.AgentID == "" {
			return model.Err("invalid_state", "an interrupted task with its original agent is required")
		}
		if task.ResumeRequest != nil && (task.ResumeRequest.State == "pending" || task.ResumeRequest.State == "accepted") {
			return model.Err("resume_pending", "the existing resume request must finish before another attempt")
		}
		if err := b.checkResumeReady(tx, task); err != nil {
			return err
		}
		if err := b.Store.AdmissionIn(tx); err != nil {
			return err
		}
		now := model.Now()
		request := model.TaskResumeRequest{ID: uuid.NewString(), TaskID: task.ID, RequesterID: p.ID, IdempotencyKey: a.IdempotencyKey, State: "pending", AgentID: task.AgentID, PreviousAssignmentID: task.AssignmentID, CreatedAt: now, UpdatedAt: now}
		if err := tx.SaveResumeRequest(request); err != nil {
			return err
		}
		task.ResumeRequest = &request
		task.State = "interrupted"
		task.LeaseUntil = 0
		if err := tx.SaveTask(task); err != nil {
			return err
		}
		if err := tx.Event(task.ID, "task.resume_requested", request); err != nil {
			return err
		}
		if slot := b.agents[task.AgentID]; slot != nil {
			slot.lastOffer = 0
		}
		out = model.TaskResumeResult{TaskID: task.ID, ResumeRequest: request}
		return nil
	})
	return out, err
}

// checkResumeReady is repeated at admission and claim. In particular, a
// pending request never turns a running or unknown command into safe work.
func (b *Broker) checkResumeReady(tx *storage.Tx, task model.Task) error {
	if task.Terminal() || task.DeadlineAt <= model.Now() {
		return model.Err("deadline_exceeded", "the original task deadline has elapsed")
	}
	if !supportsWorkspace(b.clients[task.ExecutorID], task.WorkspaceID) {
		return model.Err("executor_unavailable", "executor must reconnect for reconciliation")
	}
	commands, err := tx.Commands(task.ID)
	if err != nil {
		return err
	}
	for _, command := range commands {
		if !command.Terminal() || command.State == "unknown" {
			return model.Err("reconciliation_required", "command is running or its outcome is unknown")
		}
	}
	workspace, err := tx.Workspace(task.WorkspaceID)
	if err != nil {
		return err
	}
	if workspace.ExecutorID != task.ExecutorID {
		return model.Err("forbidden", "task workspace no longer belongs to its executor")
	}
	if workspace.Quarantined {
		return model.Err("reconciliation_required", "workspace remains quarantined")
	}
	active, err := tx.ActiveTasks()
	if err != nil {
		return err
	}
	for _, other := range active {
		if other.ID != task.ID && (other.State == "offered" || other.State == "running" || other.State == "cancel_requested") && (other.WorkspaceID == task.WorkspaceID || other.AgentID == task.AgentID) {
			return model.Err("resume_busy", "the task workspace or original agent has another active assignment")
		}
	}
	return nil
}

func (b *Broker) claimTaskResume(tx *storage.Tx, task *model.Task, p model.Principal, cid, requestID string) error {
	request, err := tx.ResumeRequest(requestID)
	if err != nil {
		return err
	}
	if request.TaskID != task.ID || request.AgentID != p.AgentID || task.AgentID != p.AgentID || task.ResumeRequest == nil || task.ResumeRequest.ID != request.ID {
		return model.Err("forbidden", "resume request does not belong to this task and original agent")
	}
	if slot := b.agents[p.AgentID]; slot == nil || slot.id != cid {
		return model.Err("stale_assignment", "current agent connection required")
	}
	if request.State == "accepted" {
		return model.Err("resume_already_claimed", "this resume request was already consumed; query its durable status")
	}
	if request.State != "pending" {
		return model.Err("resume_failed", "this resume attempt has ended; B must explicitly request a new attempt")
	}
	if task.State != "interrupted" || task.AssignmentID != request.PreviousAssignmentID {
		return model.Err("invalid_state", "matching interrupted assignment required")
	}
	if err := b.checkResumeReady(tx, *task); err != nil {
		return err
	}
	now := model.Now()
	task.AssignmentID = uuid.NewString()
	task.ConnectionID = cid
	task.LeaseEpoch++
	task.LeaseUntil = now + 60000
	task.State = "running"
	request.State = "accepted"
	request.AssignmentID = task.AssignmentID
	request.ConnectionID = cid
	request.AcceptedAt = now
	request.UpdatedAt = now
	if err := tx.SaveResumeRequest(request); err != nil {
		return err
	}
	task.ResumeRequest = &request
	if err := tx.SaveTask(*task); err != nil {
		return err
	}
	return tx.Event(task.ID, "task.resume_accepted", request)
}

func (b *Broker) reportTaskResumeFailure(tx *storage.Tx, task *model.Task, p model.Principal, cid string, a Args) (model.TaskResumeResult, error) {
	var out model.TaskResumeResult
	if !bounded(a.ResumeRequestID, 128) || !bounded(a.ErrorCode, 64) || !bounded(a.Message, 1024) || !utf8.ValidString(a.Message) || strings.TrimSpace(a.Message) == "" {
		return out, model.Err("invalid_arguments", "resume_request_id, error_code and a message of at most 1024 UTF-8 bytes are required")
	}
	for _, ch := range a.ErrorCode {
		if !(ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '_') {
			return out, model.Err("invalid_arguments", "error_code must contain lowercase ASCII letters, digits or underscores")
		}
	}
	request, err := tx.ResumeRequest(a.ResumeRequestID)
	if err != nil {
		return out, err
	}
	if request.TaskID != task.ID || request.AgentID != p.AgentID || task.AgentID != p.AgentID || task.ResumeRequest == nil || task.ResumeRequest.ID != request.ID {
		return out, model.Err("forbidden", "resume request does not belong to this task and original agent")
	}
	if slot := b.agents[p.AgentID]; slot == nil || slot.id != cid {
		return out, model.Err("stale_assignment", "current agent connection required")
	}
	if request.State == "failed" {
		return model.TaskResumeResult{TaskID: task.ID, ResumeRequest: request}, nil
	}
	if request.State == "accepted" && request.ConnectionID != cid {
		return out, model.Err("stale_assignment", "only the claiming connection may report this attempt failed")
	}
	if task.Terminal() || task.State == "cancel_requested" {
		return out, model.Err("invalid_state", "task is already terminal or cancelling")
	}
	if err := tx.FailTaskResume(task, a.ErrorCode, a.Message, "pending", "accepted"); err != nil {
		return out, err
	}
	task.State = "interrupted"
	task.LeaseUntil = 0
	if err := tx.SaveTask(*task); err != nil {
		return out, err
	}
	if err := tx.Event(task.ID, "task.resume_failed", *task.ResumeRequest); err != nil {
		return out, err
	}
	return model.TaskResumeResult{TaskID: task.ID, ResumeRequest: *task.ResumeRequest}, nil
}
