package broker

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
	"github.com/google/uuid"
)

func TestNewTaskPromptsRequireEnglishWithoutCreatingWork(t *testing.T) {
	for _, method := range []string{"tasks_submit", "tasks_run"} {
		t.Run(method, func(t *testing.T) {
			f := newFixture(t)
			notifications := 0
			f.b.agents[f.claude.AgentID].notify = func(context.Context, model.ChannelNotification) error {
				notifications++
				return nil
			}
			for i, prompt := range []string{"检查工作区", "Run tests 并报告", "Run caf\u00e9 tests", "Run tests\U0001f680", "Run\x00tests", "Run\x1btests", "Run\x7ftests", "1234!?", " \t\r\n"} {
				_, err := f.b.CallClient(f.ctx, f.peer, method, model.JSON(Args{
					WorkspaceID: "workspace-1", IdempotencyKey: fmt.Sprintf("rejected-%d", i), Prompt: prompt, WaitMS: 1,
				}))
				requireCode(t, err, "invalid_arguments")
				if !strings.Contains(model.AsError(err).Message, "English only (ASCII)") {
					t.Fatalf("missing prompt requirement: %v", err)
				}
			}
			// A scheduler pass must have nothing to offer, even with A online.
			f.b.tick(f.ctx)
			if notifications != 0 {
				t.Fatalf("rejected prompts dispatched %d channel notifications", notifications)
			}
			if err := f.store.View(f.ctx, func(tx *storage.Tx) error {
				for _, table := range []string{"tasks", "commands", "events", "assignments", "outbox"} {
					var count int
					if err := tx.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
						return err
					}
					if count != 0 {
						t.Errorf("rejected prompts created %d rows in %s", count, table)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNewEnglishTaskPromptsPreserveContentAndBounds(t *testing.T) {
	for _, method := range []string{"tasks_submit", "tasks_run"} {
		t.Run(method, func(t *testing.T) {
			f := newFixture(t)
			for i, prompt := range []string{"Inspect C:\\workspace and run tests.\r\n\tReport the result.", strings.Repeat("A", 8192)} {
				key := fmt.Sprintf("accepted-%d", i)
				_, err := f.b.CallClient(f.ctx, f.peer, method, model.JSON(Args{
					WorkspaceID: "workspace-1", IdempotencyKey: key, Prompt: prompt, WaitMS: 1,
				}))
				if err != nil {
					t.Fatal(err)
				}
				if err := f.store.View(f.ctx, func(tx *storage.Tx) error {
					task, err := tx.TaskByKey(f.executor.ID, key)
					if err == nil && (task.Prompt != prompt || task.State != "queued") {
						t.Fatalf("accepted prompt changed: task=%+v", task)
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			for _, args := range []Args{
				{WorkspaceID: "workspace-1", IdempotencyKey: "empty", Prompt: ""},
				{WorkspaceID: "workspace-1", IdempotencyKey: "oversized", Prompt: strings.Repeat("A", 8193)},
				{WorkspaceID: "workspace-1", IdempotencyKey: "", Prompt: "Valid English task."},
				{WorkspaceID: "workspace-1", IdempotencyKey: strings.Repeat("k", 201), Prompt: "Valid English task."},
			} {
				_, err := f.b.CallClient(f.ctx, f.peer, method, model.JSON(args))
				requireCode(t, err, "invalid_arguments")
			}
			f.b.tick(f.ctx)
			if err := f.store.View(f.ctx, func(tx *storage.Tx) error {
				var count int
				if err := tx.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM tasks").Scan(&count); err != nil {
					return err
				}
				if count != 2 {
					t.Fatalf("invalid boundary request created work: tasks=%d", count)
				}
				if err := tx.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM tasks WHERE state='offered'").Scan(&count); err != nil {
					return err
				}
				if count != 1 {
					t.Fatalf("valid English task was not dispatched: offered=%d", count)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLegacyTaskPromptRetriesPreserveIdempotency(t *testing.T) {
	for _, method := range []string{"tasks_submit", "tasks_run"} {
		t.Run(method, func(t *testing.T) {
			f := newFixture(t)
			const prompt = "检查之前提交的任务"
			const key = "legacy-submission"
			legacy := model.Task{
				ID: uuid.NewString(), RequesterID: f.executor.ID, WorkspaceID: "workspace-1", ExecutorID: f.executor.ExecutorID,
				Prompt: prompt, State: "queued", IdempotencyKey: key, CreatedAt: model.Now(), DeadlineAt: model.Now() + 1800000,
				RequestHash: storage.Hash(string(model.JSON(taskRequestKey{WorkspaceID: "workspace-1", Prompt: prompt, DeadlineMS: 1800000}))),
			}
			// Seed the durable state produced before the English-only policy.
			if err := f.store.Update(f.ctx, func(tx *storage.Tx) error {
				if err := tx.SaveTask(legacy); err != nil {
					return err
				}
				return tx.Event(legacy.ID, "task.submitted", model.TaskStatePayload{State: "queued"})
			}); err != nil {
				t.Fatal(err)
			}
			value, err := f.b.CallClient(f.ctx, f.peer, method, model.JSON(Args{WorkspaceID: "workspace-1", IdempotencyKey: key, Prompt: prompt, WaitMS: 1}))
			if err != nil {
				t.Fatalf("historical retry rejected: %v", err)
			}
			var id string
			switch result := value.(type) {
			case model.Task:
				id = result.ID
			case model.TaskWaitResult:
				id = result.TaskID
			}
			if id != legacy.ID || f.task(id).Prompt != prompt {
				t.Fatalf("historical retry changed task: id=%s", id)
			}
			_, err = f.b.CallClient(f.ctx, f.peer, method, model.JSON(Args{WorkspaceID: "workspace-1", IdempotencyKey: key, Prompt: "不同的历史任务", WaitMS: 1}))
			requireCode(t, err, "idempotency_conflict")
			_, err = f.b.CallClient(f.ctx, f.peer, method, model.JSON(Args{WorkspaceID: "workspace-1", IdempotencyKey: "new-key", Prompt: prompt, WaitMS: 1}))
			requireCode(t, err, "invalid_arguments")
			if err := f.store.View(f.ctx, func(tx *storage.Tx) error {
				events, err := tx.Events(legacy.ID, 0, 10)
				if len(events) != 1 {
					t.Fatalf("retry added task events: %+v", events)
				}
				var count int
				if queryErr := tx.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM tasks").Scan(&count); queryErr != nil {
					return queryErr
				}
				if count != 1 {
					t.Fatalf("historical retry duplicated tasks: %d", count)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
