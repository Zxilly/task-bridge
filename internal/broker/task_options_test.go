package broker

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
)

func TestTaskSubmissionPersistsModelAndEffort(t *testing.T) {
	f := newFixture(t)
	args := map[string]any{
		"workspace_id": "workspace-1", "prompt": "Inspect the workspace.",
		"idempotency_key": "options", "model": "opus", "effort": "high",
	}
	value, err := f.b.CallClient(f.ctx, f.peer, "tasks_submit", model.JSON(args))
	if err != nil {
		t.Fatal(err)
	}
	task := value.(model.Task)
	var saved map[string]any
	if err := json.Unmarshal(model.JSON(f.task(task.ID)), &saved); err != nil {
		t.Fatal(err)
	}
	if saved["model"] != "opus" || saved["effort"] != "high" {
		t.Fatalf("task lost model or effort: %v", saved)
	}
	retry, err := f.b.CallClient(f.ctx, f.peer, "tasks_submit", model.JSON(args))
	if err != nil || retry.(model.Task).ID != task.ID {
		t.Fatalf("identical retry changed task: %v, %v", retry, err)
	}
	for _, field := range []string{"model", "effort"} {
		original := args[field]
		if field == "model" {
			args[field] = "sonnet"
		} else {
			args[field] = "medium"
		}
		_, err := f.b.CallClient(f.ctx, f.peer, "tasks_submit", model.JSON(args))
		requireCode(t, err, "idempotency_conflict")
		delete(args, field)
		_, err = f.b.CallClient(f.ctx, f.peer, "tasks_submit", model.JSON(args))
		requireCode(t, err, "idempotency_conflict")
		args[field] = original
	}
}

func TestTaskSubmissionRejectsInvalidLaunchOptionsWithoutCreatingWork(t *testing.T) {
	f := newFixture(t)
	for i, options := range []struct{ modelName, effort string }{
		{"opus", "ultra"}, {"opus", "HIGH"}, {"opus", " high "},
		{"--print", "high"}, {" ", "high"}, {"opus\n", "high"},
		{"opus\x00", "high"}, {"opus\x7f", "high"}, {strings.Repeat("a", 1025), "high"},
	} {
		_, err := f.b.CallClient(f.ctx, f.peer, "tasks_submit", model.JSON(Args{
			WorkspaceID: "workspace-1", Prompt: "Inspect workspace.", IdempotencyKey: fmt.Sprintf("invalid-%d", i),
			Model: options.modelName, Effort: options.effort,
		}))
		requireCode(t, err, "invalid_arguments")
	}
	if err := f.store.View(f.ctx, func(tx *storage.Tx) error {
		for _, table := range []string{"tasks", "events", "assignments", "outbox"} {
			var count int
			if err := tx.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
				return err
			}
			if count != 0 {
				t.Errorf("invalid launch options created %d rows in %s", count, table)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTaskSubmissionAcceptsIndependentLaunchOptions(t *testing.T) {
	f := newFixture(t)
	for i, options := range []struct{ modelName, effort string }{
		{"", "low"}, {"", "medium"}, {"", "high"}, {"", "xhigh"}, {"", "max"},
		{"opus", ""}, {"claude-opus-4-6", ""}, {"sonnet[1m]", ""},
		{"arn:aws:bedrock:us-east-1:123456789012:inference-profile/custom-model", "high"},
	} {
		value, err := f.b.CallClient(f.ctx, f.peer, "tasks_submit", model.JSON(Args{
			WorkspaceID: "workspace-1", Prompt: "Inspect workspace.", IdempotencyKey: fmt.Sprintf("valid-%d", i),
			Model: options.modelName, Effort: options.effort,
		}))
		if err != nil {
			t.Fatal(err)
		}
		task := f.task(value.(model.Task).ID)
		if task.Model != options.modelName || task.Effort != options.effort {
			t.Fatalf("launch options changed: %+v", task)
		}
	}
}
