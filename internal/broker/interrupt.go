package broker

import (
	"strings"
	"unicode/utf8"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
)

func (b *Broker) interruptTask(tx *storage.Tx, task *model.Task, p model.Principal, cid string, a Args) error {
	if !bounded(a.ErrorCode, 64) || !bounded(a.Message, 1024) || !utf8.ValidString(a.Message) || strings.TrimSpace(a.Message) == "" {
		return model.Err("invalid_arguments", "bounded error_code and public diagnostic message required")
	}
	for _, ch := range a.ErrorCode {
		if !(ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '_') {
			return model.Err("invalid_arguments", "error_code must contain lowercase ASCII letters, digits or underscores")
		}
	}
	slot := b.agents[p.AgentID]
	if slot == nil || slot.id != cid || task.ConnectionID != cid || task.AgentID != p.AgentID || a.AssignmentID == "" || task.AssignmentID != a.AssignmentID {
		return model.Err("stale_assignment", "current connection and matching assignment required")
	}
	if task.State == "interrupted" && task.Error != nil && task.Error.Code == a.ErrorCode && task.Error.Message == a.Message {
		return nil
	}
	if err := b.authorized(*task, p, cid, a.AssignmentID); err != nil {
		return err
	}
	if task.State != "running" {
		return model.Err("invalid_state", "only a running task can be interrupted")
	}
	task.State = "interrupted"
	task.LeaseUntil = 0
	task.Error = &model.Error{Code: a.ErrorCode, Message: a.Message}
	if err := tx.FailTaskResume(task, a.ErrorCode, a.Message, "accepted"); err != nil {
		return err
	}
	if err := tx.SaveTask(*task); err != nil {
		return err
	}
	return tx.Event(task.ID, "task.interrupted", model.TaskInterruptedPayload{State: task.State, Error: task.Error})
}
