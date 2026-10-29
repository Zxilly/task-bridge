package broker

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
)

func TestTaskInterruptionFencesAssignmentWithoutChangingCommandOutcome(t *testing.T) {
	f := newFixture(t)
	task := f.running("process-failed")
	command := f.bash(task, "existing-command", "sleep 30")
	args := Args{TaskID: task.ID, AssignmentID: task.AssignmentID, ErrorCode: "claude_rate_limited", Message: "Claude Code reached an account rate limit."}
	for range 2 {
		if _, err := f.agent("task_interrupt", args); err != nil {
			t.Fatal(err)
		}
	}
	got := f.task(task.ID)
	if got.State != "interrupted" || got.LeaseUntil != 0 || got.Error == nil || got.Error.Code != args.ErrorCode {
		t.Fatalf("missing interruption: %+v", got)
	}
	if got.ConnectionID != task.ConnectionID || f.b.agents[f.claude.AgentID] == nil {
		t.Fatal("controller was disconnected")
	}
	if f.command(command.ID).State != command.State {
		t.Fatal("process failure invented a command outcome")
	}
	if _, err := f.agent("bash", Args{TaskID: task.ID, AssignmentID: task.AssignmentID, OperationKey: "late-command", Command: "echo stale"}); err == nil {
		t.Fatal("interrupted assignment retained execution authority")
	}
	var events model.EventList
	if err := f.store.View(f.ctx, func(tx *storage.Tx) error { var err error; events, err = tx.Events(task.ID, 0, 100); return err }); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.Type == "task.interrupted" {
			count++
			var payload model.TaskInterruptedPayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.Error == nil || payload.Error.Code != args.ErrorCode {
				t.Fatalf("bad durable diagnostic: %s", event.Payload)
			}
		}
	}
	if count != 1 {
		t.Fatalf("duplicate interruption events: %d", count)
	}
}

func TestTaskInterruptionRejectsStaleReporterAndClearsErrorOnExplicitResume(t *testing.T) {
	f := newFixture(t)
	task := f.running("auth-failed")
	args := Args{TaskID: task.ID, AssignmentID: task.AssignmentID, ErrorCode: "claude_authentication_failed", Message: "HTTP 403: Claude authentication was rejected."}
	for _, bad := range []Args{
		{TaskID: task.ID, AssignmentID: "old", ErrorCode: args.ErrorCode, Message: args.Message},
		{TaskID: task.ID, AssignmentID: task.AssignmentID, ErrorCode: strings.Repeat("x", 65), Message: args.Message},
		{TaskID: task.ID, AssignmentID: task.AssignmentID, ErrorCode: "bad-code", Message: args.Message},
		{TaskID: task.ID, AssignmentID: task.AssignmentID, ErrorCode: args.ErrorCode, Message: strings.Repeat("m", 1025)},
	} {
		if _, err := f.agent("task_interrupt", bad); err == nil {
			t.Fatalf("accepted invalid interruption: %+v", bad)
		}
	}
	if _, err := f.b.CallAgent(f.ctx, f.claude, "old-connection", "task_interrupt", model.JSON(args)); err == nil {
		t.Fatal("old connection interrupted live work")
	}
	if _, err := f.agent("task_interrupt", args); err != nil {
		t.Fatal(err)
	}
	if _, err := f.agent("task_resume", Args{TaskID: task.ID, PreviousAssignmentID: task.AssignmentID}); err != nil {
		t.Fatal(err)
	}
	resumed := f.task(task.ID)
	if resumed.Error != nil || resumed.State != "running" {
		t.Fatalf("new attempt retained old failure: %+v", resumed)
	}
	if _, err := f.agent("task_interrupt", args); err == nil {
		t.Fatal("old assignment overwrote a resumed task")
	}
}
