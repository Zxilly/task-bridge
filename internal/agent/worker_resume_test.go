package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func waitWorkerTask(t *testing.T, f *workerFixture, id, state string) model.Task {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		var task model.Task
		if err := f.broker.Store.View(f.ctx, func(tx *storage.Tx) (err error) { task, err = tx.Task(id); return err }); err != nil {
			t.Fatal(err)
		}
		if task.State == state {
			return task
		}
		select {
		case <-deadline.C:
			t.Fatalf("task state=%s, want %s", task.State, state)
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func stopWorkerBeforeCompletion(t *testing.T, f *workerFixture, accept bool) (model.Task, Session) {
	t.Helper()
	task := f.submit(t, "saved-task", "Keep this task conversation isolated and recoverable")
	var saved Session
	f.worker.launcher = launcherFunc(func(ctx context.Context, session Session) error {
		saved = session
		if session.Resume {
			return errors.New("new task unexpectedly resumed a session")
		}
		client, err := taskClient(ctx, session)
		if err != nil {
			return err
		}
		defer client.Close()
		if accept {
			result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "task_accept", Arguments: struct {
				TaskID       string `json:"task_id"`
				AssignmentID string `json:"assignment_id"`
			}{session.Task.TaskID, session.Task.AssignmentID}})
			if err != nil {
				return err
			}
			if result.IsError {
				return errors.New("initial accept rejected")
			}
		}
		if err := os.WriteFile(filepath.Join(session.Directory, "retained-context.txt"), []byte("original task context"), 0600); err != nil {
			return err
		}
		return errors.New("controlled session interruption")
	})
	if err := f.worker.Run(f.ctx); err == nil || !strings.Contains(err.Error(), "controlled session interruption") {
		t.Fatalf("worker did not report the interruption: %v", err)
	}
	state := "queued"
	if accept {
		state = "interrupted"
	}
	task = waitWorkerTask(t, f, task.ID, state)
	if _, err := os.Stat(saved.Directory); err != nil {
		t.Fatalf("unfinished session directory was not preserved: %v", err)
	}
	return task, saved
}

func completeResumedWorkerSession(t *testing.T, f *workerFixture, task model.Task, previous Session, offered bool) {
	t.Helper()
	restarted, err := New(f.worker.cfg, f.worker.log)
	if err != nil {
		t.Fatal(err)
	}
	launches := 0
	restarted.launcher = launcherFunc(func(ctx context.Context, session Session) error {
		launches++
		if !session.Resume || session.ID != previous.ID || session.Directory != previous.Directory || session.Token == previous.Token || session.Task.AssignmentID == previous.Task.AssignmentID {
			return errors.New("resume lost its original context or reused old relay authorization")
		}
		data, err := os.ReadFile(filepath.Join(session.Directory, "retained-context.txt"))
		if err != nil || string(data) != "original task context" {
			return errors.New("resume lost its saved working directory")
		}
		if !strings.Contains(session.Prompt, session.Task.AssignmentID) || !strings.Contains(session.Prompt, "obsolete") || strings.Contains(session.Prompt, "Keep this task conversation") {
			return errors.New("resume did not replace stale assignment instructions")
		}
		client, err := taskClient(ctx, session)
		if err != nil {
			return err
		}
		defer client.Close()
		if offered {
			result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "task_accept", Arguments: struct {
				TaskID       string `json:"task_id"`
				AssignmentID string `json:"assignment_id"`
			}{session.Task.TaskID, session.Task.AssignmentID}})
			if err != nil {
				return err
			}
			if result.IsError {
				return errors.New("resumed offer could not be accepted")
			}
		}
		result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "task_get", Arguments: model.TaskGetArgs{TaskID: task.ID}})
		if err != nil {
			return err
		}
		current, err := decodeTool[model.TaskSnapshot](result)
		if err != nil || current.Task.State != "running" || current.Task.AssignmentID != session.Task.AssignmentID {
			return errors.New("broker did not issue a current running assignment")
		}
		result, err = client.CallTool(ctx, &mcp.CallToolParams{Name: "task_progress", Arguments: struct {
			TaskID       string `json:"task_id"`
			AssignmentID string `json:"assignment_id"`
			Message      string `json:"message"`
		}{task.ID, previous.Task.AssignmentID, "stale history must not authorize work"}})
		if err != nil {
			return err
		}
		if !result.IsError {
			return errors.New("old assignment from conversation history was accepted")
		}
		result, err = client.CallTool(ctx, &mcp.CallToolParams{Name: "task_complete", Arguments: struct {
			TaskID       string `json:"task_id"`
			AssignmentID string `json:"assignment_id"`
			Outcome      string `json:"outcome"`
			Summary      string `json:"summary"`
		}{task.ID, session.Task.AssignmentID, "succeeded", "resumed original conversation"}})
		if err != nil {
			return err
		}
		if result.IsError {
			return errors.New("resumed task completion rejected")
		}
		return nil
	})
	if err := restarted.Resume(f.ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	if launches != 1 {
		t.Fatalf("resume launched %d sessions", launches)
	}
	waitWorkerTask(t, f, task.ID, "succeeded")
	if _, err := os.Stat(previous.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("terminal task retained its local session state: %v", err)
	}
}

func TestWorkerResumePreservesConversationAndRefreshesAssignment(t *testing.T) {
	f := setupWorker(t)
	task, previous := stopWorkerBeforeCompletion(t, f, true)
	completeResumedWorkerSession(t, f, task, previous, false)
}

func TestWorkerResumePreservesUnacceptedConversation(t *testing.T) {
	f := setupWorker(t)
	task, previous := stopWorkerBeforeCompletion(t, f, false)
	// Ordinary run cannot replace a recoverable conversation with a new UUID.
	f.worker.launcher = launcherFunc(func(context.Context, Session) error {
		t.Error("ordinary run overwrote a saved task conversation")
		return errors.New("unexpected fresh session")
	})
	if err := f.worker.Run(f.ctx); err == nil || !strings.Contains(err.Error(), "resume --task-id") {
		t.Fatalf("missing explicit resume requirement: %v", err)
	}
	waitWorkerTask(t, f, task.ID, "queued")
	completeResumedWorkerSession(t, f, task, previous, true)
}

func TestWorkerResumeRefusesUnknownCommandBeforeStartingClaude(t *testing.T) {
	f := setupWorker(t)
	task, previous := stopWorkerBeforeCompletion(t, f, true)
	if err := f.broker.Store.Update(f.ctx, func(tx *storage.Tx) error {
		return tx.SaveCommand(model.Command{ID: uuid.NewString(), TaskID: task.ID, AssignmentID: task.AssignmentID, OperationKey: "unknown-operation", State: "unknown", Command: "unresolved execution"})
	}); err != nil {
		t.Fatal(err)
	}
	f.worker.launcher = launcherFunc(func(context.Context, Session) error {
		t.Error("Claude started before unknown execution was reconciled")
		return errors.New("unexpected resume")
	})
	err := f.worker.Resume(f.ctx, task.ID)
	if err == nil || model.AsError(err).Code != "reconciliation_required" {
		t.Fatalf("unknown execution was not rejected: %v", err)
	}
	current := waitWorkerTask(t, f, task.ID, "interrupted")
	if current.AssignmentID != task.AssignmentID {
		t.Fatal("refused resume changed the broker assignment")
	}
	if _, err := os.Stat(previous.Directory); err != nil {
		t.Fatal("refused resume removed recoverable context")
	}
}

func TestWorkerResumeRefusesMissingSavedSession(t *testing.T) {
	f := setupWorker(t)
	task, previous := stopWorkerBeforeCompletion(t, f, true)
	if err := os.Remove(filepath.Join(previous.Directory, sessionManifestName)); err != nil {
		t.Fatal(err)
	}
	f.worker.launcher = launcherFunc(func(context.Context, Session) error {
		t.Error("missing saved session silently fell back to a fresh conversation")
		return errors.New("unexpected fallback")
	})
	if err := f.worker.Resume(f.ctx, task.ID); err == nil || !strings.Contains(err.Error(), "no matching saved Claude session") {
		t.Fatalf("missing saved context was not rejected: %v", err)
	}
	if current := waitWorkerTask(t, f, task.ID, "interrupted"); current.AssignmentID != task.AssignmentID {
		t.Fatal("missing saved context changed the broker assignment")
	}
}

func TestWorkerResumeChecksOfferedAssignmentBeforeStartingClaude(t *testing.T) {
	f := setupWorker(t)
	task, _ := stopWorkerBeforeCompletion(t, f, false)
	token, err := f.worker.cfg.Token()
	if err != nil {
		t.Fatal(err)
	}
	owner, err := f.broker.Authenticate(f.ctx, token)
	if err != nil {
		t.Fatal(err)
	}
	const ownerConnection = "original-agent-connection"
	if err := f.broker.AgentConnected(f.ctx, owner, ownerConnection, func(context.Context, model.ChannelNotification) error { return nil }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.broker.AgentDisconnected(owner, ownerConnection) })
	task = waitWorkerTask(t, f, task.ID, "offered")
	_, otherToken, err := f.broker.CreateDevice(f.ctx, model.Principal{Kind: "claude", AgentID: "other-agent", WorkspaceID: task.WorkspaceID})
	if err != nil {
		t.Fatal(err)
	}
	otherTokenFile := filepath.Join(t.TempDir(), "other-agent.token")
	if err := os.WriteFile(otherTokenFile, []byte(otherToken), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := f.worker.cfg
	cfg.TokenFile = otherTokenFile
	other, err := New(cfg, f.worker.log)
	if err != nil {
		t.Fatal(err)
	}
	launches := 0
	other.launcher = launcherFunc(func(context.Context, Session) error {
		launches++
		return errors.New("original conversation launched with another agent credential")
	})
	err = other.Resume(f.ctx, task.ID)
	if launches != 0 {
		t.Fatal("another agent's token started the task's saved Claude conversation")
	}
	if err == nil || model.AsError(err).Code != "stale_assignment" {
		t.Fatalf("missing offered-assignment authorization error: %v", err)
	}
	current := waitWorkerTask(t, f, task.ID, "offered")
	if current.AssignmentID != task.AssignmentID || current.ConnectionID != ownerConnection {
		t.Fatal("refused resume changed the original agent's assignment")
	}
}

func TestWorkerResumeCleansTerminalStateWithoutLaunchingClaude(t *testing.T) {
	f := setupWorker(t)
	task, previous := stopWorkerBeforeCompletion(t, f, true)
	if _, err := f.broker.Admin(f.ctx, "tasks.abandon", model.JSON(model.AdminArgs{ID: task.ID, Reason: "reviewed and closed interrupted task"})); err != nil {
		t.Fatal(err)
	}
	f.worker.launcher = launcherFunc(func(context.Context, Session) error {
		t.Error("terminal task started Claude again")
		return errors.New("unexpected terminal resume")
	})
	if err := f.worker.Resume(f.ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	waitWorkerTask(t, f, task.ID, "abandoned")
	if _, err := os.Stat(previous.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale terminal session directory survived: %v", err)
	}
}
