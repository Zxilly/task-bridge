package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/client"
	"github.com/Zxilly/task-bridge/internal/mcpagent"
	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
	"github.com/coder/websocket"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type loadPeer struct {
	executorID  string
	workspaceID string
	root        string
	b           *client.Client
	a           *mcp.ClientSession
	offers      chan model.ChannelNotification
	task        model.Task
	assignment  string
	commandID   string
	command     string
}

func startLoadExecutor(t *testing.T, ctx context.Context, cfg client.Config) *client.Client {
	t.Helper()
	c, err := client.New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("executor %s: %v", cfg.ExecutorID, err)
		}
		if err := c.Close(); err != nil {
			t.Errorf("close executor %s: %v", cfg.ExecutorID, err)
		}
	})
	return c
}

func connectLoadAgent(t *testing.T, ctx context.Context, url, token string, offers chan model.ChannelNotification) *mcp.ClientSession {
	t.Helper()
	socket, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols: []string{"mcp"},
		HTTPHeader:   http.Header{"Authorization": []string{"Bearer " + token}},
	})
	if err != nil {
		t.Fatal(err)
	}
	a, err := mcp.NewClient(&mcp.Implementation{Name: "small-scale-A-protocol-test-not-Claude", Version: "1"}, nil).
		Connect(ctx, observeTransport{mcpagent.NewTransport(socket), offers}, nil)
	if err != nil {
		_ = socket.CloseNow()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func waitLoadConnections(t *testing.T, f *fixture, agents, executors int) {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		a, b := f.broker.ConnectionCounts()
		if a == agents && b == executors {
			return
		}
		select {
		case <-f.ctx.Done():
			t.Fatalf("connections A=%d B=%d, want A=%d B=%d: %v", a, b, agents, executors, f.ctx.Err())
		case <-ticker.C:
		}
	}
}

func loadSubmit(t *testing.T, ctx context.Context, p *loadPeer, key, prompt string) model.Task {
	t.Helper()
	raw, err := p.b.RPC(ctx, "tasks_submit", map[string]any{
		"workspace_id": p.workspaceID, "idempotency_key": key, "prompt": prompt, "deadline_ms": 60000,
	})
	if err != nil {
		t.Fatal(err)
	}
	var task model.Task
	if err := json.Unmarshal(raw, &task); err != nil {
		t.Fatal(err)
	}
	if task.ID == "" || task.WorkspaceID != p.workspaceID || task.ExecutorID != p.executorID || task.Terminal() {
		t.Fatalf("submission was not durable and bound to its B: %+v", task)
	}
	return task
}

func loadAccept(t *testing.T, ctx context.Context, p *loadPeer, expectedTaskID string) string {
	t.Helper()
	for {
		select {
		case n := <-p.offers:
			meta := n.Meta
			if meta.WorkspaceID != p.workspaceID {
				t.Fatalf("agent received another workspace's offer: %+v", n)
			}
			if meta.TaskID == p.task.ID && expectedTaskID != p.task.ID {
				continue // A delayed duplicate of the already completed first offer.
			}
			if meta.TaskID != expectedTaskID {
				t.Fatalf("agent received an unexpected task: %+v", n)
			}
			assignment := meta.AssignmentID
			if assignment == "" {
				t.Fatal("offer omitted its assignment")
			}
			tool(t, ctx, p.a, "task_accept", map[string]any{"task_id": expectedTaskID, "assignment_id": assignment})
			return assignment
		case <-ctx.Done():
			t.Fatal("load-test offer not received:", ctx.Err())
			return ""
		}
	}
}

func requireLoadToolError(t *testing.T, ctx context.Context, a *mcp.ClientSession, name string, args any, code string) {
	t.Helper()
	r, err := a.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	if !r.IsError || len(r.Content) != 1 {
		t.Fatalf("%s unexpectedly succeeded: %+v", name, r)
	}
	text, ok := r.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("%s returned non-text error", name)
	}
	var body struct {
		Error model.Error `json:"error"`
	}
	if err := json.Unmarshal([]byte(text.Text), &body); err != nil || body.Error.Code != code {
		t.Fatalf("%s error = %s, want %s; decode=%v", name, text.Text, code, err)
	}
}

// This is a small protocol simulation with real WS/SQLite/Bash components. Its
// sampled process memory and elapsed time are observations, not capacity limits.
func TestSmallScaleFiveAgentsTenExecutors(t *testing.T) {
	started := time.Now()
	var baseline, connected, final runtime.MemStats
	runtime.ReadMemStats(&baseline)
	baselineGoroutines := runtime.NumGoroutine()
	f := setup(t)
	peers := make([]loadPeer, 10)
	peers[0] = loadPeer{executorID: f.cfg.ExecutorID, workspaceID: "demo", root: f.root, b: f.b, a: f.a, offers: f.offers}
	agentURL := strings.TrimSuffix(f.cfg.ServerURL, "/v1/client") + "/mcp/agent"
	root := t.TempDir()
	for i := 1; i < len(peers); i++ {
		p := &peers[i]
		p.executorID = fmt.Sprintf("load-b-%02d", i)
		p.workspaceID = fmt.Sprintf("load-ws-%02d", i)
		p.root = filepath.Join(root, p.workspaceID)
		if err := os.MkdirAll(p.root, 0700); err != nil {
			t.Fatal(err)
		}
		if err := f.broker.CreateWorkspace(f.ctx, model.Workspace{ID: p.workspaceID, ExecutorID: p.executorID}); err != nil {
			t.Fatal(err)
		}
		_, token, err := f.broker.CreateDevice(f.ctx, model.Principal{Kind: "executor", ExecutorID: p.executorID, WorkspaceID: p.workspaceID})
		if err != nil {
			t.Fatal(err)
		}
		tokenPath := filepath.Join(root, p.executorID+".token")
		if err := os.WriteFile(tokenPath, []byte(token), 0600); err != nil {
			t.Fatal(err)
		}
		cfg := f.cfg
		cfg.ExecutorID = p.executorID
		cfg.JournalPath = filepath.Join(root, p.executorID+"-journal.db")
		cfg.TokenFile = tokenPath
		cfg.Workspaces = map[string]client.WorkspaceConfig{p.workspaceID: {Root: p.root, BashPath: bash(t)}}
		p.b = startLoadExecutor(t, f.ctx, cfg)
		if i < 5 {
			_, tokenA, err := f.broker.CreateDevice(f.ctx, model.Principal{Kind: "claude", AgentID: fmt.Sprintf("load-a-%02d", i), WorkspaceID: p.workspaceID})
			if err != nil {
				t.Fatal(err)
			}
			p.offers = make(chan model.ChannelNotification, 32)
			p.a = connectLoadAgent(t, f.ctx, agentURL, tokenA, p.offers)
		}
	}
	waitLoadConnections(t, f, 5, 10)
	runtime.ReadMemStats(&connected)
	ids := map[string]bool{}
	for i := range peers {
		p := &peers[i]
		p.task = loadSubmit(t, f.ctx, p, "same-key-in-independent-devices", "Exercise only "+p.workspaceID)
		if ids[p.task.ID] {
			t.Fatal("separate executor submissions collapsed to the same task")
		}
		ids[p.task.ID] = true
	}
	for i := range peers {
		other := peers[(i+1)%len(peers)].task.ID
		_, err := peers[i].b.RPC(f.ctx, "tasks_get", map[string]any{"task_id": other})
		var denied *model.Error
		if !errors.As(err, &denied) || denied.Code != "forbidden" {
			t.Fatalf("B %d accessed another workspace: %v", i, err)
		}
		if i < 5 {
			requireLoadToolError(t, f.ctx, peers[i].a, "task_get", map[string]any{"task_id": other}, "forbidden")
			peers[i].assignment = loadAccept(t, f.ctx, &peers[i], peers[i].task.ID)
		}
	}
	followup := loadSubmit(t, f.ctx, &peers[0], "serial-followup", "Remain queued until this workspace's previous task is completed.")
	assertTaskStates := func(completed bool) {
		t.Helper()
		if err := f.store.View(f.ctx, func(tx *storage.Tx) error {
			active := map[string]int{}
			for i, p := range peers {
				task, err := tx.Task(p.task.ID)
				if err != nil {
					return err
				}
				want := "queued"
				if i < 5 {
					want = "running"
					if completed {
						want = "succeeded"
					}
				}
				if task.State != want || task.ExecutorID != p.executorID || task.WorkspaceID != p.workspaceID {
					return fmt.Errorf("task for executor %s = %+v, want %s", p.executorID, task, want)
				}
				if task.State == "running" || task.State == "offered" {
					active[task.WorkspaceID]++
				}
			}
			if !completed {
				task, err := tx.Task(followup.ID)
				if err != nil {
					return err
				}
				if task.State != "queued" || task.AssignmentID != "" {
					return fmt.Errorf("workspace offered another task before its predecessor completed: %+v", task)
				}
			}
			for workspace, count := range active {
				if count > 1 {
					return fmt.Errorf("workspace %s has %d active tasks", workspace, count)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	assertTaskStates(false)
	for i := 0; i < 5; i++ {
		p := &peers[i]
		marker := fmt.Sprintf("workspace-proof-%02d", i)
		p.command = "sleep 1; printf '%s\\n' '" + marker + "' >> load-proof.txt; printf '%s' '" + marker + "'"
		args := map[string]any{"task_id": p.task.ID, "assignment_id": p.assignment, "operation_key": "one-logical-command", "command": p.command, "wait_ms": 1}
		result := tool(t, f.ctx, p.a, "bash", args)
		p.commandID = result["command"].(map[string]any)["command_id"].(string)
		if i == 0 {
			requireLoadToolError(t, f.ctx, p.a, "bash", map[string]any{
				"task_id": p.task.ID, "assignment_id": p.assignment, "operation_key": "must-not-run-concurrently", "command": "printf bad > forbidden-overlap.txt", "wait_ms": 1,
			}, "command_busy")
		}
	}
	for i := 0; i < 5; i++ {
		p := &peers[i]
		result := tool(t, f.ctx, p.a, "command_wait", map[string]any{"task_id": p.task.ID, "command_id": p.commandID, "wait_ms": 10000})
		command := result["command"].(map[string]any)
		if result["pending"] != false || command["state"] != "exited" || command["exit_code"] != float64(0) {
			t.Fatalf("Bash failed for %s: %+v", p.workspaceID, result)
		}
		again := tool(t, f.ctx, p.a, "bash", map[string]any{"task_id": p.task.ID, "assignment_id": p.assignment, "operation_key": "one-logical-command", "command": p.command, "wait_ms": 10000})
		if again["command"].(map[string]any)["command_id"] != p.commandID {
			t.Fatal("load-test retry created a second command")
		}
	}
	// A command finishing does not release the workspace before task_complete.
	assertTaskStates(false)
	for i := 0; i < 5; i++ {
		p := &peers[i]
		tool(t, f.ctx, p.a, "task_complete", map[string]any{"task_id": p.task.ID, "assignment_id": p.assignment, "outcome": "succeeded", "summary": "Completed one isolated B command."})
	}
	followupAssignment := loadAccept(t, f.ctx, &peers[0], followup.ID)
	tool(t, f.ctx, peers[0].a, "task_complete", map[string]any{"task_id": followup.ID, "assignment_id": followupAssignment, "outcome": "succeeded", "summary": "Verified sequential task handoff after predecessor completion."})
	assertTaskStates(true)
	for i, p := range peers {
		path := filepath.Join(p.root, "load-proof.txt")
		data, err := os.ReadFile(path)
		if i < 5 {
			want := fmt.Sprintf("workspace-proof-%02d\n", i)
			if err != nil || string(data) != want {
				t.Fatalf("B workspace %s was not modified exactly once: %q, %v", p.workspaceID, data, err)
			}
		} else if !os.IsNotExist(err) {
			t.Fatalf("unassigned B workspace %s executed work: %q, %v", p.workspaceID, data, err)
		}
	}
	if _, err := os.Stat(filepath.Join(peers[0].root, "forbidden-overlap.txt")); !os.IsNotExist(err) {
		t.Fatal("a second command ran concurrently on the same task")
	}
	waitLoadConnections(t, f, 5, 10)
	runtime.ReadMemStats(&final)
	t.Logf("small-scale protocol simulation (not real Claude): A=5 B=10 primary_completed=5 queued_without_A=5 serial_followup_completed=1 real_Bash_runs=5 elapsed=%s", time.Since(started).Round(time.Millisecond))
	t.Logf("Go process samples: heap_alloc_bytes baseline=%d connected=%d final=%d; heap_inuse_bytes=%d; sys_bytes=%d; goroutines baseline=%d final=%d; runtime=%s %s/%s", baseline.HeapAlloc, connected.HeapAlloc, final.HeapAlloc, final.HeapInuse, final.Sys, baselineGoroutines, runtime.NumGoroutine(), runtime.Version(), runtime.GOOS, runtime.GOARCH)
}
