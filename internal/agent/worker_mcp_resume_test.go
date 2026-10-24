package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/broker"
	"github.com/Zxilly/task-bridge/internal/client"
	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/server"
	"github.com/Zxilly/task-bridge/internal/storage"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type workerMCPResumeFixture struct {
	ctx        context.Context
	broker     *broker.Broker
	worker     *Worker
	host       *mcp.ClientSession
	workspace  string
	journal    string
	workerDone chan struct{}
	workerErr  error
}

// This fixture never creates a fake executor peer: B binds its own journal
// stream through the real authenticated WebSocket handshake.
func setupWorkerMCPResume(t *testing.T) *workerMCPResumeFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	directory := t.TempDir()
	store, err := storage.Open(ctx, filepath.Join(directory, "broker.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	b := broker.New(store, 20)
	if err := b.CreateWorkspace(ctx, model.Workspace{ID: "workspace", ExecutorID: "executor"}); err != nil {
		t.Fatal(err)
	}
	_, tokenB, err := b.CreateDevice(ctx, model.Principal{Kind: "executor", ExecutorID: "executor", WorkspaceID: "workspace"})
	if err != nil {
		t.Fatal(err)
	}
	_, tokenA, err := b.CreateDevice(ctx, model.Principal{Kind: "claude", AgentID: "agent", WorkspaceID: "workspace"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(server.NewHandler(b))
	brokerCtx, stopBroker := context.WithCancel(ctx)
	brokerDone := make(chan struct{})
	go func() { defer close(brokerDone); b.Run(brokerCtx) }()
	t.Cleanup(func() { b.Drain(); stopBroker(); <-brokerDone; server.Close() })
	workspace := filepath.Join(directory, "B-workspace")
	if err := os.Mkdir(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	tokenBPath, tokenAPath := filepath.Join(directory, "B.token"), filepath.Join(directory, "A.token")
	if err := os.WriteFile(tokenBPath, []byte(tokenB), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenAPath, []byte(tokenA), 0600); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(directory, "B-journal.db")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	bClient, err := client.New(client.Config{
		ServerURL: "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/client", ExecutorID: "executor",
		JournalPath: journal, TokenFile: tokenBPath,
		Workspaces: map[string]client.WorkspaceConfig{"workspace": {Root: workspace, BashPath: workerMCPBash(t)}},
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	bCtx, stopB := context.WithCancel(ctx)
	bDone := make(chan error, 1)
	go func() { bDone <- bClient.Run(bCtx) }()
	t.Cleanup(func() {
		stopB()
		if err := <-bDone; err != nil {
			t.Errorf("B shutdown: %v", err)
		}
		_ = bClient.Close()
	})
	serverTransport, hostTransport := mcp.NewInMemoryTransports()
	bServer, err := bClient.MCPServer().Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bServer.Close() })
	host, err := mcp.NewClient(&mcp.Implementation{Name: "real-B-resume-host", Version: "1"}, nil).Connect(ctx, hostTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	worker, err := New(Config{
		ServerURL: "ws" + strings.TrimPrefix(server.URL, "http") + "/mcp/agent", TokenFile: tokenAPath,
		ClaudePath: executable, StateDir: filepath.Join(directory, "A-state"),
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	f := &workerMCPResumeFixture{ctx: ctx, broker: b, worker: worker, host: host, workspace: workspace, journal: journal}
	workerMCPTool[model.TaskListResult](t, f, "tasks_list", model.TaskListArgs{WorkspaceID: "workspace", Limit: 1})
	return f
}

func workerMCPBash(t *testing.T) string {
	t.Helper()
	if configured := os.Getenv("BRIDGE_TEST_BASH"); configured != "" {
		return configured
	}
	if runtime.GOOS == "windows" {
		for _, path := range []string{`C:\Program Files\Git\bin\bash.exe`, `C:\Program Files\Git\usr\bin\bash.exe`} {
			if _, err := os.Stat(path); err == nil {
				return path
			}
		}
	}
	path, err := exec.LookPath("bash")
	if err != nil {
		t.Fatal("real B recovery tests require Bash; set BRIDGE_TEST_BASH")
	}
	return path
}

func (f *workerMCPResumeFixture) start(t *testing.T, launch launcherFunc) {
	t.Helper()
	f.worker.launcher = launch
	ctx, cancel := context.WithCancel(f.ctx)
	f.workerDone = make(chan struct{})
	go func() { f.workerErr = f.worker.Run(ctx); close(f.workerDone) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-f.workerDone:
			if f.workerErr != nil {
				t.Errorf("A worker shutdown: %v", f.workerErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("A worker did not stop")
		}
	})
}

func workerMCPTool[T any](t *testing.T, f *workerMCPResumeFixture, name string, args any) T {
	t.Helper()
	result, err := f.host.CallTool(f.ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("B %s: %v", name, err)
	}
	value, err := decodeTool[T](result)
	if err != nil {
		t.Fatalf("B %s result: %v", name, err)
	}
	return value
}

func workerRelayTool[T any](ctx context.Context, session *mcp.ClientSession, name string, args any) (T, error) {
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		var value T
		return value, err
	}
	return decodeTool[T](result)
}

func (f *workerMCPResumeFixture) waitTask(t *testing.T, id, description string, matches func(model.Task) bool) model.Task {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		task := workerMCPTool[model.TaskSnapshot](t, f, "tasks_get", model.TaskGetArgs{TaskID: id}).Task
		if matches(task) {
			return task
		}
		select {
		case <-f.workerDone:
			t.Fatalf("A worker exited while waiting for %s: %v", description, f.workerErr)
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s: task=%s receipt=%+v", description, task.State, task.ResumeRequest)
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (f *workerMCPResumeFixture) waitConnected(t *testing.T) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		if executors, agents := f.broker.ConnectionCounts(); executors == 1 && agents == 1 {
			return
		}
		select {
		case <-f.workerDone:
			t.Fatalf("original A Run exited instead of staying online: %v", f.workerErr)
		case <-deadline.C:
			t.Fatal("A did not reconnect to await a B recovery request")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (f *workerMCPResumeFixture) nextSession(t *testing.T, sessions <-chan Session) Session {
	t.Helper()
	select {
	case session := <-sessions:
		return session
	case <-f.workerDone:
		t.Fatalf("A exited before starting the requested session: %v", f.workerErr)
	case <-time.After(10 * time.Second):
		t.Fatal("requested Claude session did not start")
	}
	return Session{}
}

func (f *workerMCPResumeFixture) noSession(t *testing.T, sessions <-chan Session) {
	t.Helper()
	select {
	case <-sessions:
		t.Fatal("A launched another session without a new B recovery request")
	case <-f.workerDone:
		t.Fatalf("A Run stopped while awaiting B requests: %v", f.workerErr)
	case <-time.After(150 * time.Millisecond):
	}
}

func emitWorkerMCPSession(ctx context.Context, sessions chan<- Session, session Session) error {
	select {
	case sessions <- session:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestWorkerMCPResumeUsesSameOnlineRunAndOriginalConversation(t *testing.T) {
	f := setupWorkerMCPResume(t)
	sessions := make(chan Session, 8)
	releaseResume := make(chan struct{})
	var launches atomic.Int64
	f.start(t, func(ctx context.Context, session Session) error {
		attempt := launches.Add(1)
		relay, err := taskClient(ctx, session)
		if err != nil {
			return err
		}
		defer relay.Close()
		if !session.Resume {
			if _, err := workerRelayTool[model.Task](ctx, relay, "task_accept", map[string]string{"task_id": session.Task.TaskID, "assignment_id": session.Task.AssignmentID}); err != nil {
				return err
			}
		}
		if attempt == 1 {
			command, err := workerRelayTool[model.CommandResult](ctx, relay, "bash", map[string]any{
				"task_id": session.Task.TaskID, "assignment_id": session.Task.AssignmentID,
				"operation_key": "one-original-execution", "command": "printf 'once\\n' >> resume-proof.txt", "wait_ms": 10000,
			})
			if err != nil {
				return err
			}
			if command.Pending || command.Command.State != "exited" || command.Command.ExitCode == nil || *command.Command.ExitCode != 0 {
				return errors.New("real B did not durably finish the original command")
			}
			if err := os.WriteFile(filepath.Join(session.Directory, "retained-context.txt"), []byte("original task context"), 0600); err != nil {
				return err
			}
		}
		if session.Resume {
			if data, err := os.ReadFile(filepath.Join(session.Directory, "retained-context.txt")); err != nil || string(data) != "original task context" {
				return errors.New("resumed task lost its original working context")
			}
		}
		if err := emitWorkerMCPSession(ctx, sessions, session); err != nil {
			return err
		}
		if attempt == 1 {
			return errors.New("controlled first Claude failure")
		}
		if attempt == 2 {
			select {
			case <-releaseResume:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		_, err = workerRelayTool[model.Task](ctx, relay, "task_complete", map[string]string{
			"task_id": session.Task.TaskID, "assignment_id": session.Task.AssignmentID, "outcome": "succeeded", "summary": "MCP recovery complete",
		})
		return err
	})
	task := workerMCPTool[model.Task](t, f, "tasks_submit", model.TaskSubmitArgs{WorkspaceID: "workspace", Prompt: "Keep this original task conversation for explicit recovery.", IdempotencyKey: "original-task", DeadlineMS: 45000, Model: "opus", Effort: "high"})
	original := f.nextSession(t, sessions)
	if original.Model != "opus" || original.Effort != "high" {
		t.Fatalf("initial task lost its model or effort: %+v", original)
	}
	f.waitTask(t, task.ID, "initial interruption", func(task model.Task) bool { return task.State == "interrupted" })
	f.waitConnected(t)
	f.noSession(t, sessions)
	if _, err := os.Stat(original.Directory); err != nil {
		t.Fatalf("unfinished task context was removed: %v", err)
	}
	request := model.TaskResumeArgs{TaskID: task.ID, IdempotencyKey: "restore-original"}
	receipt := workerMCPTool[model.TaskResumeResult](t, f, "tasks_resume", request)
	replay := workerMCPTool[model.TaskResumeResult](t, f, "tasks_resume", request)
	if receipt.ResumeRequest.ID == "" || replay.ResumeRequest.ID != receipt.ResumeRequest.ID || receipt.TaskID != task.ID {
		t.Fatal("retry did not return the original task's durable recovery receipt")
	}
	resumed := f.nextSession(t, sessions)
	if resumed.Model != original.Model || resumed.Effort != original.Effort {
		t.Fatalf("resuming changed the task model or effort: %+v", resumed)
	}
	if !resumed.Resume || resumed.ID != original.ID || resumed.Directory != original.Directory || resumed.Task.TaskID != task.ID || resumed.Token == original.Token || resumed.Task.AssignmentID == original.Task.AssignmentID || resumed.Task.ResumeRequestID != receipt.ResumeRequest.ID {
		t.Fatal("MCP recovery replaced the conversation or reused stale relay authorization")
	}
	if stale, err := taskClient(f.ctx, original); err == nil {
		_ = stale.Close()
		t.Fatal("old task token authenticated against the resumed relay")
	}
	current := f.waitTask(t, task.ID, "accepted receipt before task completion", func(task model.Task) bool {
		return task.State == "running" && task.ResumeRequest != nil && task.ResumeRequest.ID == receipt.ResumeRequest.ID && task.ResumeRequest.State == "accepted"
	})
	if current.Terminal() {
		t.Fatal("accepting a recovery request completed the task")
	}
	f.noSession(t, sessions)
	close(releaseResume)
	f.waitTask(t, task.ID, "resumed task completion", func(task model.Task) bool { return task.State == "succeeded" })
	fresh := workerMCPTool[model.Task](t, f, "tasks_submit", model.TaskSubmitArgs{WorkspaceID: "workspace", Prompt: "Start another independent task after recovery completes.", IdempotencyKey: "next-fresh-task", DeadlineMS: 45000})
	next := f.nextSession(t, sessions)
	if next.Resume || next.Task.TaskID != fresh.ID || next.ID == original.ID || next.Directory == original.Directory {
		t.Fatal("next task reused the recovered task conversation")
	}
	f.waitTask(t, fresh.ID, "next independent task completion", func(task model.Task) bool { return task.State == "succeeded" })
	if _, err := os.Stat(original.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("terminal original task retained local state: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(f.workspace, "resume-proof.txt")); err != nil || string(data) != "once\n" {
		t.Fatalf("recovery repeated or lost the known B execution: %q, %v", data, err)
	}
	if _, err := os.Stat(f.journal); err != nil {
		t.Fatalf("real B did not retain its SQLite journal: %v", err)
	}
	replayedAfterCompletion := workerMCPTool[model.TaskResumeResult](t, f, "tasks_resume", request)
	if replayedAfterCompletion.ResumeRequest.ID != receipt.ResumeRequest.ID {
		t.Fatal("replaying a completed task's resume key created another request")
	}
	f.noSession(t, sessions)
	if launches.Load() != 3 {
		t.Fatalf("expected original, resumed and fresh child; got %d launches", launches.Load())
	}
}

func TestWorkerMCPResumeRequiresNewRequestAfterFailureAndMissingManifest(t *testing.T) {
	f := setupWorkerMCPResume(t)
	sessions := make(chan Session, 8)
	var launches atomic.Int64
	f.start(t, func(ctx context.Context, session Session) error {
		launches.Add(1)
		relay, err := taskClient(ctx, session)
		if err != nil {
			return err
		}
		defer relay.Close()
		if !session.Resume {
			if _, err := workerRelayTool[model.Task](ctx, relay, "task_accept", map[string]string{"task_id": session.Task.TaskID, "assignment_id": session.Task.AssignmentID}); err != nil {
				return err
			}
		}
		if err := emitWorkerMCPSession(ctx, sessions, session); err != nil {
			return err
		}
		return errors.New("controlled repeated Claude failure")
	})
	task := workerMCPTool[model.Task](t, f, "tasks_submit", model.TaskSubmitArgs{WorkspaceID: "workspace", Prompt: "Keep failed recovery attempts explicit and durable.", IdempotencyKey: "failed-task", DeadlineMS: 45000})
	original := f.nextSession(t, sessions)
	f.waitTask(t, task.ID, "original task interruption", func(task model.Task) bool { return task.State == "interrupted" })
	f.waitConnected(t)
	f.noSession(t, sessions)
	var previousRequestID string
	for _, key := range []string{"failed-attempt-1", "failed-attempt-2"} {
		args := model.TaskResumeArgs{TaskID: task.ID, IdempotencyKey: key}
		receipt := workerMCPTool[model.TaskResumeResult](t, f, "tasks_resume", args)
		resumed := f.nextSession(t, sessions)
		if !resumed.Resume || resumed.ID != original.ID || resumed.Directory != original.Directory || resumed.Token == original.Token || resumed.Task.ResumeRequestID != receipt.ResumeRequest.ID || receipt.ResumeRequest.ID == previousRequestID {
			t.Fatal("new explicit recovery attempt did not preserve only the original conversation")
		}
		f.waitTask(t, task.ID, "failed recovery receipt", func(task model.Task) bool {
			return task.State == "interrupted" && task.ResumeRequest != nil && task.ResumeRequest.ID == receipt.ResumeRequest.ID && task.ResumeRequest.State == "failed" && task.ResumeRequest.Error != nil
		})
		f.waitConnected(t)
		replay := workerMCPTool[model.TaskResumeResult](t, f, "tasks_resume", args)
		if replay.ResumeRequest.ID != receipt.ResumeRequest.ID || replay.ResumeRequest.State != "failed" {
			t.Fatal("same key did not retain its original failed receipt")
		}
		f.noSession(t, sessions)
		previousRequestID = receipt.ResumeRequest.ID
	}
	if launches.Load() != 3 {
		t.Fatalf("failed receipt replay unexpectedly launched another child: %d", launches.Load())
	}
	if err := os.Remove(filepath.Join(original.Directory, sessionManifestName)); err != nil {
		t.Fatal(err)
	}
	missing := workerMCPTool[model.TaskResumeResult](t, f, "tasks_resume", model.TaskResumeArgs{TaskID: task.ID, IdempotencyKey: "missing-manifest"})
	f.waitTask(t, task.ID, "missing manifest failure receipt", func(task model.Task) bool {
		return task.State == "interrupted" && task.ResumeRequest != nil && task.ResumeRequest.ID == missing.ResumeRequest.ID && task.ResumeRequest.State == "failed" && task.ResumeRequest.Error != nil
	})
	f.waitConnected(t)
	f.noSession(t, sessions)
	if launches.Load() != 3 {
		t.Fatal("missing saved context silently fell back to a fresh child")
	}
	old := workerMCPTool[model.TaskResumeResult](t, f, "tasks_resume", model.TaskResumeArgs{TaskID: task.ID, IdempotencyKey: "failed-attempt-1"})
	if old.ResumeRequest.State != "failed" || old.ResumeRequest.ID == missing.ResumeRequest.ID {
		t.Fatal("new requests overwrote the first durable recovery receipt")
	}
}
