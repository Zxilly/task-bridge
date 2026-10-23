package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/broker"
	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/server"
	"github.com/Zxilly/task-bridge/internal/storage"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type launcherFunc func(context.Context, Session) error

func (f launcherFunc) Launch(ctx context.Context, session Session) error { return f(ctx, session) }

type workerFixture struct {
	ctx    context.Context
	broker *broker.Broker
	peer   *broker.ClientPeer
	worker *Worker
}

func setupWorker(t *testing.T) *workerFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	directory := t.TempDir()
	store, err := storage.Open(ctx, filepath.Join(directory, "broker.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	b := broker.New(store, 10)
	if err := b.CreateWorkspace(ctx, model.Workspace{ID: "workspace", ExecutorID: "executor"}); err != nil {
		t.Fatal(err)
	}
	p, _, err := b.CreateDevice(ctx, model.Principal{Kind: "executor", ExecutorID: "executor", WorkspaceID: "workspace"})
	if err != nil {
		t.Fatal(err)
	}
	_, token, err := b.CreateDevice(ctx, model.Principal{Kind: "claude", AgentID: "agent", WorkspaceID: "workspace"})
	if err != nil {
		t.Fatal(err)
	}
	peer, err := b.ConnectClient(ctx, p, "stream", func(model.Envelope) error { return nil }, func() {}, "workspace")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(server.NewHandler(b))
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); b.Run(runCtx) }()
	t.Cleanup(func() { b.Drain(); stop(); <-done; server.Close() })
	tokenFile := filepath.Join(directory, "agent.token")
	if err := os.WriteFile(tokenFile, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	w, err := New(Config{ServerURL: "ws" + strings.TrimPrefix(server.URL, "http") + "/mcp/agent", TokenFile: tokenFile, ClaudePath: executable, StateDir: filepath.Join(directory, "agent-state")}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return &workerFixture{ctx: ctx, broker: b, peer: peer, worker: w}
}

func (f *workerFixture) submit(t *testing.T, key, prompt string) model.Task {
	t.Helper()
	result, err := f.broker.CallClient(f.ctx, f.peer, "tasks_submit", model.JSON(model.TaskSubmitArgs{WorkspaceID: "workspace", Prompt: prompt, IdempotencyKey: key, DeadlineMS: 30000}))
	if err != nil {
		t.Fatal(err)
	}
	return result.(model.Task)
}

func taskClient(ctx context.Context, session Session) (*mcp.ClientSession, error) {
	var config struct {
		MCPServers map[string]struct {
			URL string `json:"url"`
		} `json:"mcpServers"`
	}
	data, err := os.ReadFile(session.MCPConfig)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	return mcp.NewClient(&mcp.Implementation{Name: "test-Claude-process", Version: "1"}, nil).Connect(ctx, &mcp.StreamableClientTransport{Endpoint: config.MCPServers["bridge"].URL, HTTPClient: &http.Client{Transport: taskTokenTransport{session.Token}}}, nil)
}

type taskTokenTransport struct{ token string }

func (t taskTokenTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	copy := request.Clone(request.Context())
	copy.Header.Set("Authorization", "Bearer "+t.token)
	return http.DefaultTransport.RoundTrip(copy)
}

func TestWorkerStartsIsolatedSessionsAndWaitsForPreviousChild(t *testing.T) {
	f := setupWorker(t)
	sessions := make(chan Session, 2)
	completed := make(chan string, 2)
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseFirst) }) })
	firstTask := f.submit(t, "first", "first-task-private-context")
	f.worker.launcher = launcherFunc(func(ctx context.Context, session Session) error {
		sessions <- session
		client, err := taskClient(ctx, session)
		if err != nil {
			return err
		}
		defer client.Close()
		for _, call := range []struct {
			name string
			args json.RawMessage
		}{
			{"task_accept", model.JSON(struct {
				TaskID       string `json:"task_id"`
				AssignmentID string `json:"assignment_id"`
			}{session.Task.TaskID, session.Task.AssignmentID})},
			{"task_complete", model.JSON(struct {
				TaskID       string `json:"task_id"`
				AssignmentID string `json:"assignment_id"`
				Outcome      string `json:"outcome"`
				Summary      string `json:"summary"`
			}{session.Task.TaskID, session.Task.AssignmentID, "succeeded", "isolated result"})},
		} {
			result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: call.name, Arguments: call.args})
			if err != nil {
				return err
			}
			if result.IsError {
				return errors.New("task tool was rejected")
			}
		}
		completed <- session.Task.TaskID
		if session.Task.TaskID == firstTask.ID {
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	ctx, cancel := context.WithCancel(f.ctx)
	done := make(chan error, 1)
	go func() { done <- f.worker.Run(ctx) }()
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Errorf("worker shutdown: %v", err)
			}
		})
	}
	t.Cleanup(stop)
	var first Session
	select {
	case first = <-sessions:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	select {
	case <-completed:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	secondTask := f.submit(t, "second", "second-task-only-context")
	select {
	case <-sessions:
		t.Fatal("next task started before prior child exited")
	case <-time.After(100 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(releaseFirst) })
	var second Session
	select {
	case second = <-sessions:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	if second.Task.TaskID != secondTask.ID || first.ID == second.ID || first.Directory == second.Directory || first.Token == second.Token {
		t.Fatal("tasks shared a session, directory, or capability")
	}
	if strings.Contains(second.Prompt, "first-task-private-context") || !strings.Contains(second.Prompt, "second-task-only-context") {
		t.Fatal("task context leaked across sessions")
	}
	if _, err := os.Stat(first.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old session directory survived: %v", err)
	}
	select {
	case <-completed:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	stop()
}

func TestWorkerDisconnectsWhenChildExitsWithoutCompletion(t *testing.T) {
	f := setupWorker(t)
	task := f.submit(t, "crash", "do not silently retry")
	f.worker.launcher = launcherFunc(func(ctx context.Context, session Session) error {
		client, err := taskClient(ctx, session)
		if err != nil {
			return err
		}
		defer client.Close()
		result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "task_accept", Arguments: model.JSON(struct {
			TaskID       string `json:"task_id"`
			AssignmentID string `json:"assignment_id"`
		}{session.Task.TaskID, session.Task.AssignmentID})})
		if err != nil {
			return err
		}
		if result.IsError {
			return errors.New("accept rejected")
		}
		return errors.New("controlled child failure")
	})
	if err := f.worker.Run(f.ctx); err == nil || !strings.Contains(err.Error(), "controlled child failure") {
		t.Fatalf("missing failure: %v", err)
	}
	for {
		var state string
		err := f.broker.Store.View(f.ctx, func(tx *storage.Tx) error { task, err := tx.Task(task.ID); state = task.State; return err })
		if err != nil {
			t.Fatal(err)
		}
		if state == "interrupted" {
			break
		}
		select {
		case <-f.ctx.Done():
			t.Fatalf("task never interrupted: %s", state)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestWorkerStopsChildWhenAssignmentChanges(t *testing.T) {
	f := setupWorker(t)
	task := f.submit(t, "expired-offer", "stop the old task session")
	sessions := make(chan Session, 2)
	childExited := make(chan struct{}, 2)
	f.worker.launcher = launcherFunc(func(ctx context.Context, session Session) error {
		client, err := taskClient(ctx, session)
		if err != nil {
			return err
		}
		defer client.Close()
		result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "task_get", Arguments: model.TaskGetArgs{TaskID: session.Task.TaskID}})
		if err != nil {
			return err
		}
		if result.IsError {
			return errors.New("task snapshot rejected")
		}
		sessions <- session
		<-ctx.Done()
		childExited <- struct{}{}
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(f.ctx)
	finished := make(chan struct{})
	var workerErr error
	go func() {
		workerErr = f.worker.Run(ctx)
		close(finished)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("worker cleanup did not finish")
		}
	})
	var original Session
	select {
	case original = <-sessions:
	case <-finished:
		t.Fatalf("worker stopped before starting the old session: %v", workerErr)
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	if err := f.broker.Store.Update(f.ctx, func(tx *storage.Tx) error {
		current, err := tx.Task(task.ID)
		if err != nil {
			return err
		}
		if current.AssignmentID != original.Task.AssignmentID || current.State != "offered" {
			return errors.New("task did not retain its original offer")
		}
		// Simulate the broker reoffering after its unaccepted lease expires.
		current.AssignmentID = uuid.NewString()
		return tx.SaveTask(current)
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
		if workerErr == nil || !strings.Contains(workerErr.Error(), "assignment changed") {
			t.Fatalf("missing assignment-change failure: %v", workerErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("old child survived the changed task assignment")
	}
	select {
	case <-childExited:
	default:
		t.Fatal("worker returned without waiting for the old child to exit")
	}
	select {
	case <-sessions:
		t.Fatal("worker started a new session after losing its assignment")
	default:
	}
}
