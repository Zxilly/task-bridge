package broker

import (
	"context"
	"encoding/json"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
	"github.com/google/uuid"
)

// Recovery is explicitly enabled by the controller, never by a Claude task.
// Persist it on accepted tasks so an unplanned broker restart can recover too.
func (b *Broker) configureAgentRecovery(ctx context.Context, p model.Principal, cid string, raw json.RawMessage) (model.AgentRecoveryConfig, error) {
	var args struct {
		AutoResume *bool `json:"auto_resume"`
	}
	if err := decodeUsageArgs(raw, &args); err != nil {
		return model.AgentRecoveryConfig{}, err
	}
	if args.AutoResume == nil {
		return model.AgentRecoveryConfig{}, model.Err("invalid_arguments", "auto_resume is required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.Check(ctx, p); err != nil {
		return model.AgentRecoveryConfig{}, err
	}
	slot := b.agents[p.AgentID]
	if slot == nil || slot.id != cid || slot.p.ID != p.ID {
		return model.AgentRecoveryConfig{}, model.Err("stale_connection", "current agent connection required")
	}
	slot.autoResume = *args.AutoResume
	return model.AgentRecoveryConfig{AutoResume: slot.autoResume}, nil
}

func queueAutomaticResume(tx *storage.Tx, task *model.Task) error {
	if task.State != "interrupted" || task.Error != nil || task.DeadlineAt <= model.Now() {
		return nil
	}
	if task.ResumeRequest != nil && task.ResumeRequest.State == "pending" {
		return nil
	}
	// Revocation is never interpreted as permission to start again.
	owner, err := tx.Principal(task.RequesterID)
	if err != nil {
		return err
	}
	if owner.Disabled {
		return nil
	}
	now := model.Now()
	request := model.TaskResumeRequest{Automatic: true, ID: uuid.NewString(), TaskID: task.ID, RequesterID: task.RequesterID, IdempotencyKey: "auto-reconnect/" + task.ID + "/" + task.AssignmentID, State: "pending", AgentID: task.AgentID, PreviousAssignmentID: task.AssignmentID, CreatedAt: now, UpdatedAt: now}
	if err := tx.SaveResumeRequest(request); err != nil {
		return err
	}
	task.ResumeRequest = &request
	if err := tx.SaveTask(*task); err != nil {
		return err
	}
	return tx.Event(task.ID, "task.resume_requested", request)
}
