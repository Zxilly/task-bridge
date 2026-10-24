package storage

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/Zxilly/task-bridge/internal/model"
)

func TestResumeRequestHistoryPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "resume.db")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	task := model.Task{ID: "task", RequesterID: "requester", IdempotencyKey: "submission", WorkspaceID: "workspace", AgentID: "agent", State: "interrupted"}
	first := model.TaskResumeRequest{ID: "first", TaskID: task.ID, RequesterID: task.RequesterID, IdempotencyKey: "first-key", State: "failed", AgentID: task.AgentID, Error: &model.Error{Code: "local_failure", Message: "Original conversation unavailable"}}
	second := model.TaskResumeRequest{ID: "second", TaskID: task.ID, RequesterID: task.RequesterID, IdempotencyKey: "second-key", State: "pending", AgentID: task.AgentID}
	err = store.Update(ctx, func(tx *Tx) error {
		if err := tx.SaveTask(task); err != nil {
			return err
		}
		for _, request := range []model.TaskResumeRequest{first, second} {
			if err := tx.SaveResumeRequest(request); err != nil {
				return err
			}
		}
		task.ResumeRequest = &second
		return tx.SaveTask(task)
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.View(ctx, func(tx *Tx) error {
		old, err := tx.ResumeRequestByKey(task.RequesterID, first.IdempotencyKey)
		if err != nil {
			return err
		}
		if old.ID != first.ID || old.State != "failed" || old.Error == nil || old.Error.Code != first.Error.Code {
			t.Fatalf("history did not persist: %+v", old)
		}
		latest, err := tx.Task(task.ID)
		if err != nil {
			return err
		}
		if latest.ResumeRequest == nil || latest.ResumeRequest.ID != second.ID {
			t.Fatalf("latest summary lost: %+v", latest)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(ctx, func(tx *Tx) error {
		duplicate := second
		duplicate.ID = "third"
		return tx.SaveResumeRequest(duplicate)
	}); err == nil {
		t.Fatal("duplicate requester/key did not fail its unique constraint")
	}
	if err := store.Update(ctx, func(tx *Tx) error {
		orphan := second
		orphan.ID, orphan.TaskID, orphan.IdempotencyKey = "orphan", "missing", "orphan-key"
		return tx.SaveResumeRequest(orphan)
	}); err == nil {
		t.Fatal("orphan request bypassed foreign key")
	}
}

func TestResumeRequestAndTaskFailureRollBackTogether(t *testing.T) {
	ctx := context.Background()
	store := testStore(t, filepath.Join(t.TempDir(), "resume.db"))
	task := model.Task{ID: "task", RequesterID: "requester", IdempotencyKey: "submission", WorkspaceID: "workspace", AgentID: "agent", State: "interrupted"}
	request := model.TaskResumeRequest{ID: "request", TaskID: task.ID, RequesterID: task.RequesterID, IdempotencyKey: "resume-key", State: "pending", AgentID: task.AgentID}
	if err := store.Update(ctx, func(tx *Tx) error {
		if err := tx.SaveTask(task); err != nil {
			return err
		}
		if err := tx.SaveResumeRequest(request); err != nil {
			return err
		}
		task.ResumeRequest = &request
		return tx.SaveTask(task)
	}); err != nil {
		t.Fatal(err)
	}
	interrupted := errors.New("interrupt transaction")
	err := store.Update(ctx, func(tx *Tx) error {
		current, err := tx.Task(task.ID)
		if err != nil {
			return err
		}
		if err = tx.FailTaskResume(&current, "cancelled", "Task cancelled", "pending"); err != nil {
			return err
		}
		current.State = "cancelled"
		if err = tx.SaveTask(current); err != nil {
			return err
		}
		return interrupted
	})
	if !errors.Is(err, interrupted) {
		t.Fatalf("rollback error=%v", err)
	}
	if err := store.View(ctx, func(tx *Tx) error {
		current, err := tx.Task(task.ID)
		if err != nil {
			return err
		}
		history, err := tx.ResumeRequest(request.ID)
		if err != nil {
			return err
		}
		if current.State != "interrupted" || current.ResumeRequest.State != "pending" || history.State != "pending" {
			t.Fatal("failed transaction partially consumed request")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreFailsPendingAndAcceptedResumeReceipts(t *testing.T) {
	for _, state := range []string{"pending", "accepted"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			store := testStore(t, filepath.Join(t.TempDir(), "resume.db"))
			task := model.Task{ID: "task", RequesterID: "requester", IdempotencyKey: "submission", WorkspaceID: "workspace", AgentID: "agent", State: "interrupted"}
			request := model.TaskResumeRequest{ID: "request", TaskID: task.ID, RequesterID: task.RequesterID, IdempotencyKey: "resume-key", State: state, AgentID: task.AgentID}
			if err := store.Update(ctx, func(tx *Tx) error {
				if err := tx.SaveWorkspace(model.Workspace{ID: task.WorkspaceID, ExecutorID: "executor"}); err != nil {
					return err
				}
				if err := tx.SaveTask(task); err != nil {
					return err
				}
				if err := tx.SaveResumeRequest(request); err != nil {
					return err
				}
				task.ResumeRequest = &request
				return tx.SaveTask(task)
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.ResetRestore(ctx); err != nil {
				t.Fatal(err)
			}
			if err := store.View(ctx, func(tx *Tx) error {
				current, err := tx.Task(task.ID)
				if err != nil {
					return err
				}
				history, err := tx.ResumeRequest(request.ID)
				if err != nil {
					return err
				}
				if current.ResumeRequest.State != "failed" || history.State != "failed" || history.Error == nil || history.Error.Code != "broker_restored" {
					t.Fatalf("restored request could replay: %+v", history)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
