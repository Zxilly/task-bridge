// These tests exercise the Go implementation with the official MCP SDK, real
// WebSocket connections, real SQLite journals, and real Bash subprocesses.
// The A peer is a protocol test client, not a Claude account.
package integration

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/broker"
	"github.com/Zxilly/task-bridge/internal/client"
	"github.com/Zxilly/task-bridge/internal/mcpagent"
	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/server"
	"github.com/Zxilly/task-bridge/internal/storage"
	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type observeTransport struct {
	mcp.Transport
	offers chan map[string]any
}
type observeConnection struct {
	mcp.Connection
	offers chan map[string]any
}

func (t observeTransport) Connect(ctx context.Context) (mcp.Connection, error) {
	c, e := t.Transport.Connect(ctx)
	return observeConnection{c, t.offers}, e
}
func (c observeConnection) Read(ctx context.Context) (jsonrpc.Message, error) {
	for {
		m, e := c.Connection.Read(ctx)
		if e != nil {
			return nil, e
		}
		if req, ok := m.(*jsonrpc.Request); ok && req.Method == "notifications/claude/channel" {
			var p map[string]any
			if e = json.Unmarshal(req.Params, &p); e != nil {
				return nil, e
			}
			select {
			case c.offers <- p:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			continue
		}
		return m, nil
	}
}

type fixture struct {
	ctx    context.Context
	store  *storage.Store
	broker *broker.Broker
	b      *client.Client
	a      *mcp.ClientSession
	host   *mcp.ClientSession
	offers chan map[string]any
	root   string
	tokenA string
	cfg    client.Config
	stopB  func()
}

func bash(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("BRIDGE_TEST_BASH"); p != "" {
		return p
	}
	if runtime.GOOS == "windows" {
		for _, p := range []string{`C:\Program Files\Git\bin\bash.exe`, `C:\Program Files\Git\usr\bin\bash.exe`} {
			if _, e := os.Stat(p); e == nil {
				return p
			}
		}
	}
	p, e := exec.LookPath("bash")
	if e != nil {
		t.Fatal("Bash required for integration tests; set BRIDGE_TEST_BASH")
	}
	return p
}
func setup(t *testing.T) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	dir := t.TempDir()
	s, e := storage.Open(ctx, filepath.Join(dir, "server.db"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	b := broker.New(s, 100)
	if e = b.CreateWorkspace(ctx, model.Workspace{ID: "demo", ExecutorID: "b1"}); e != nil {
		t.Fatal(e)
	}
	_, tokenB, e := b.CreateDevice(ctx, model.Principal{Kind: "executor", ExecutorID: "b1", WorkspaceID: "demo"})
	if e != nil {
		t.Fatal(e)
	}
	_, tokenA, e := b.CreateDevice(ctx, model.Principal{Kind: "claude", AgentID: "a1", WorkspaceID: "demo"})
	if e != nil {
		t.Fatal(e)
	}
	runCtx, stop := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() { defer close(runDone); b.Run(runCtx) }()
	srv := httptest.NewServer(server.NewHandler(b))
	t.Cleanup(func() { b.Drain(); stop(); <-runDone; srv.Close() })
	root := filepath.Join(dir, "B-workspace")
	if e = os.MkdirAll(root, 0700); e != nil {
		t.Fatal(e)
	}
	tokenPath := filepath.Join(dir, "b-token")
	if e = os.WriteFile(tokenPath, []byte(tokenB), 0600); e != nil {
		t.Fatal(e)
	}
	cfg := client.Config{ServerURL: "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/client", ExecutorID: "b1", JournalPath: filepath.Join(dir, "client.db"), TokenFile: tokenPath, Workspaces: map[string]client.WorkspaceConfig{"demo": {Root: root, BashPath: bash(t)}}, MaxOutputBytes: 10 << 20}
	bc, e := client.New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if e != nil {
		t.Fatal(e)
	}
	clientCtx, stopClient := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- bc.Run(clientCtx) }()
	var stopOnce sync.Once
	stopB := func() {
		stopOnce.Do(func() {
			stopClient()
			if e := <-done; e != nil {
				t.Log(e)
			}
			bc.Close()
		})
	}
	t.Cleanup(stopB)
	ctransport, stransport := mcp.NewInMemoryTransports()
	hostServer, e := bc.MCPServer().Connect(ctx, stransport, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { hostServer.Close() })
	host, e := mcp.NewClient(&mcp.Implementation{Name: "B-mcp-host-test", Version: "1"}, nil).Connect(ctx, ctransport, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { host.Close() })
	socket, _, e := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/mcp/agent", &websocket.DialOptions{Subprotocols: []string{"mcp"}, HTTPHeader: http.Header{"Authorization": []string{"Bearer " + tokenA}}})
	if e != nil {
		t.Fatal(e)
	}
	offers := make(chan map[string]any, 32)
	a, e := mcp.NewClient(&mcp.Implementation{Name: "A-protocol-test-not-Claude", Version: "1"}, nil).Connect(ctx, observeTransport{mcpagent.NewTransport(socket), offers}, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { a.Close() })
	return &fixture{ctx, s, b, bc, a, host, offers, root, tokenA, cfg, stopB}
}
func tool(t *testing.T, ctx context.Context, s *mcp.ClientSession, name string, args any) map[string]any {
	t.Helper()
	r, e := s.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if e != nil {
		t.Fatal(name, e)
	}
	if r.IsError {
		t.Fatalf("%s error: %+v", name, r.Content)
	}
	serialized, e := json.Marshal(r)
	if e != nil || len(serialized) > model.MaxToolBytes {
		t.Fatalf("%s result exceeds limit: %d %v", name, len(serialized), e)
	}
	var raw []byte
	if r.StructuredContent != nil {
		raw = model.JSON(r.StructuredContent)
	} else if len(r.Content) > 0 {
		raw = []byte(r.Content[0].(*mcp.TextContent).Text)
	}
	var out map[string]any
	if e = json.Unmarshal(raw, &out); e != nil {
		t.Fatalf("%s invalid result: %s (%v)", name, raw, e)
	}
	return out
}
func offer(t *testing.T, f *fixture) map[string]any {
	t.Helper()
	select {
	case n := <-f.offers:
		return n["meta"].(map[string]any)
	case <-f.ctx.Done():
		t.Fatal("channel notification not received", f.ctx.Err())
		return nil
	}
}
func accept(t *testing.T, f *fixture) (string, string) {
	t.Helper()
	o := offer(t, f)
	id := o["task_id"].(string)
	assignment := o["assignment_id"].(string)
	tool(t, f.ctx, f.a, "task_accept", map[string]any{"task_id": id, "assignment_id": assignment})
	return id, assignment
}

func TestTaskChannelBashAndMCPWaitComplete(t *testing.T) {
	f := setup(t)
	submitted := tool(t, f.ctx, f.host, "tasks_submit", map[string]any{"workspace_id": "demo", "prompt": "Inspect the remote workspace, change a file, run a check and report completion.", "idempotency_key": "workflow-1"})
	id := submitted["task_id"].(string)
	taskID, assignment := accept(t, f)
	if taskID != id {
		t.Fatal("channel task mismatch")
	}
	waiting := make(chan map[string]any, 1)
	waitErr := make(chan error, 1)
	go func() {
		r, e := f.host.CallTool(f.ctx, &mcp.CallToolParams{Name: "tasks_wait", Arguments: map[string]any{"task_id": id, "wait_ms": 15000}})
		if e != nil {
			waitErr <- e
			return
		}
		var v map[string]any
		if r.StructuredContent != nil {
			e = json.Unmarshal(model.JSON(r.StructuredContent), &v)
		} else {
			e = json.Unmarshal([]byte(r.Content[0].(*mcp.TextContent).Text), &v)
		}
		if e != nil {
			waitErr <- e
			return
		}
		waiting <- v
	}()
	marker := uuid.NewString()
	args := func(key, command string) map[string]any {
		return map[string]any{"task_id": id, "assignment_id": assignment, "operation_key": key, "command": command, "wait_ms": 10000}
	}
	inspect := tool(t, f.ctx, f.a, "bash", args("inspect", "pwd; hostname; printf '"+marker+"'"))
	if !strings.Contains(inspect["stdout"].(string), marker) {
		t.Fatalf("missing output %+v", inspect)
	}
	change := args("change", "printf '%s\\n' '"+marker+"' >> bridge-proof.txt")
	first := tool(t, f.ctx, f.a, "bash", change)
	again := tool(t, f.ctx, f.a, "bash", change)
	if first["command"].(map[string]any)["command_id"] != again["command"].(map[string]any)["command_id"] {
		t.Fatal("command id changed on retry")
	}
	check := tool(t, f.ctx, f.a, "bash", args("check", `test "$(wc -l < bridge-proof.txt)" -eq 1 && printf 'check passed'`))
	if check["command"].(map[string]any)["exit_code"].(float64) != 0 {
		t.Fatalf("check failed %+v", check)
	}
	content, e := os.ReadFile(filepath.Join(f.root, "bridge-proof.txt"))
	if e != nil || strings.TrimSpace(string(content)) != marker {
		t.Fatalf("command did not affect B workspace exactly once: %q %v", content, e)
	}
	select {
	case <-waiting:
		t.Fatal("wait returned before task was complete")
	case e := <-waitErr:
		t.Fatal(e)
	default:
	}
	tool(t, f.ctx, f.a, "task_progress", map[string]any{"task_id": id, "assignment_id": assignment, "message": "Changed the B file and verified its contents."})
	tool(t, f.ctx, f.a, "task_complete", map[string]any{"task_id": id, "assignment_id": assignment, "outcome": "succeeded", "summary": "Verified remote workspace changes.", "result": map[string]any{"marker": marker}})
	select {
	case result := <-waiting:
		if result["pending"] != false {
			t.Fatalf("still pending %+v", result)
		}
	case e := <-waitErr:
		t.Fatal(e)
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	repeated := tool(t, f.ctx, f.host, "tasks_submit", map[string]any{"workspace_id": "demo", "prompt": "Inspect the remote workspace, change a file, run a check and report completion.", "idempotency_key": "workflow-1"})
	if repeated["task_id"] != id {
		t.Fatal("submission duplicated")
	}
	second := tool(t, f.ctx, f.host, "tasks_submit", map[string]any{"workspace_id": "demo", "prompt": "Second sequential task.", "idempotency_key": "workflow-2"})
	secondID, secondAssignment := accept(t, f)
	if secondID != second["task_id"] {
		t.Fatal("next task not scheduled")
	}
	tool(t, f.ctx, f.a, "task_complete", map[string]any{"task_id": secondID, "assignment_id": secondAssignment, "outcome": "succeeded", "summary": "Second task complete."})
}

func TestLargeOutputIsPagedAndLossless(t *testing.T) {
	f := setup(t)
	tool(t, f.ctx, f.host, "tasks_submit", map[string]any{"workspace_id": "demo", "prompt": "Return large output.", "idempotency_key": "large"})
	id, assignment := accept(t, f)
	r := tool(t, f.ctx, f.a, "bash", map[string]any{"task_id": id, "assignment_id": assignment, "operation_key": "large", "command": `printf '中文'; head -c 60000 /dev/zero | tr '\0' x`, "wait_ms": 10000})
	cmd := r["command"].(map[string]any)["command_id"].(string)
	var got []byte
	cursor := int64(0)
	for n := 0; n < 1000; n++ {
		page := tool(t, f.ctx, f.a, "command_output", map[string]any{"task_id": id, "command_id": cmd, "cursor": cursor, "limit": 8192})
		for _, v := range page["chunks"].([]any) {
			chunk := v.(map[string]any)
			data, e := base64.StdEncoding.DecodeString(chunk["data"].(string))
			if e != nil {
				t.Fatal(e)
			}
			got = append(got, data...)
		}
		next := int64(page["cursor"].(float64))
		if page["has_more"] == false {
			break
		}
		if next <= cursor {
			t.Fatal("output cursor did not advance")
		}
		cursor = next
	}
	if string(got) != "中文"+strings.Repeat("x", 60000) {
		t.Fatalf("output corrupted or truncated: %d bytes", len(got))
	}
}

func TestTaskCancellationStopsBProcess(t *testing.T) {
	f := setup(t)
	tool(t, f.ctx, f.host, "tasks_submit", map[string]any{"workspace_id": "demo", "prompt": "Wait for cancellation.", "idempotency_key": "cancel"})
	id, assignment := accept(t, f)
	r := tool(t, f.ctx, f.a, "bash", map[string]any{"task_id": id, "assignment_id": assignment, "operation_key": "sleep", "command": "sleep 30; printf 'must-not-run' > late.txt", "wait_ms": 1})
	if r["pending"] != true {
		t.Fatalf("expected running command %+v", r)
	}
	tool(t, f.ctx, f.host, "tasks_cancel", map[string]any{"task_id": id, "reason": "integration cancellation"})
	result := tool(t, f.ctx, f.host, "tasks_wait", map[string]any{"task_id": id, "wait_ms": 10000})
	if result["pending"] != false {
		t.Fatal("cancellation not confirmed")
	}
	snapshot := result["snapshot"].(map[string]any)
	if snapshot["task"].(map[string]any)["state"] != "cancelled" {
		t.Fatalf("wrong state %+v", snapshot)
	}
	if _, e := os.Stat(filepath.Join(f.root, "late.txt")); !os.IsNotExist(e) {
		t.Fatal("cancelled process completed its side effect")
	}
}

func TestSnapshotRollbackReconcilesBJournalWithoutReexecution(t *testing.T) {
	f := setup(t)
	tool(t, f.ctx, f.host, "tasks_submit", map[string]any{"workspace_id": "demo", "prompt": "Recover this task from a snapshot.", "idempotency_key": "recover"})
	id, assignment := accept(t, f)
	command := `sleep 1; printf 'once\n' >> recovery-proof.txt; printf 'recovered-output'`
	first := tool(t, f.ctx, f.a, "bash", map[string]any{"task_id": id, "assignment_id": assignment, "operation_key": "before-backup", "command": command, "wait_ms": 1})
	cmdID := first["command"].(map[string]any)["command_id"].(string)
	backup := filepath.Join(t.TempDir(), "snapshot.db")
	if e := f.store.Backup(f.ctx, backup); e != nil {
		t.Fatal(e)
	}
	tool(t, f.ctx, f.a, "command_wait", map[string]any{"task_id": id, "command_id": cmdID, "wait_ms": 10000})
	post := tool(t, f.ctx, f.a, "bash", map[string]any{"task_id": id, "assignment_id": assignment, "operation_key": "after-backup", "command": `printf 'post-backup\n' >> post-backup.txt`, "wait_ms": 10000})
	orphanID := post["command"].(map[string]any)["command_id"].(string)
	f.stopB()
	f.a.Close()
	restoredPath := filepath.Join(t.TempDir(), "restored.db")
	if e := storage.RestoreNew(f.ctx, backup, restoredPath); e != nil {
		t.Fatal(e)
	}
	s, e := storage.Open(f.ctx, restoredPath)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	b := broker.New(s, 100)
	if _, e = b.Authenticate(f.ctx, f.tokenA); e == nil {
		t.Fatal("restored device credentials were not invalidated")
	}
	_, tokenB, e := b.CreateDevice(f.ctx, model.Principal{Kind: "executor", ExecutorID: "b1", WorkspaceID: "demo"})
	if e != nil {
		t.Fatal(e)
	}
	_, tokenA, e := b.CreateDevice(f.ctx, model.Principal{Kind: "claude", AgentID: "a1", WorkspaceID: "demo"})
	if e != nil {
		t.Fatal(e)
	}
	ctx, stop := context.WithCancel(f.ctx)
	done := make(chan struct{})
	go func() { defer close(done); b.Run(ctx) }()
	srv := httptest.NewServer(server.NewHandler(b))
	t.Cleanup(func() { b.Drain(); stop(); <-done; srv.Close() })
	cfg := f.cfg
	cfg.ServerURL = "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/client"
	if e = os.WriteFile(cfg.TokenFile, []byte(tokenB), 0600); e != nil {
		t.Fatal(e)
	}
	reconciled, e := client.Reconcile(f.ctx, cfg, "verified stopped executor; compare retained journal with restored snapshot")
	if e != nil {
		t.Fatal("reconcile:", e)
	}
	if reconciled.KnownCommands != 1 || reconciled.OrphanCommands != 1 {
		t.Fatalf("wrong reconciliation classification %+v", reconciled)
	}
	c, e := client.New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if e != nil {
		t.Fatal(e)
	}
	clientCtx, stopClient := context.WithCancel(f.ctx)
	clientDone := make(chan error, 1)
	go func() { clientDone <- c.Run(clientCtx) }()
	t.Cleanup(func() { stopClient(); <-clientDone; c.Close() })
	for {
		var bytes int64
		e = s.View(f.ctx, func(tx *storage.Tx) error {
			cmd, e := tx.Command(cmdID)
			if e == nil {
				bytes = cmd.OutputBytes
			}
			return e
		})
		if e != nil {
			t.Fatal(e)
		}
		if bytes == int64(len("recovered-output")) {
			break
		}
		select {
		case <-f.ctx.Done():
			t.Fatal("retained output did not replay", f.ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	if _, e = b.Admin(f.ctx, "workspaces.release", model.JSON(map[string]any{"id": "demo", "reason": "old executor stopped; known result replayed; orphan effects inspected"})); e != nil {
		t.Fatal(e)
	}
	socket, _, e := websocket.Dial(f.ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/mcp/agent", &websocket.DialOptions{Subprotocols: []string{"mcp"}, HTTPHeader: http.Header{"Authorization": []string{"Bearer " + tokenA}}})
	if e != nil {
		t.Fatal(e)
	}
	offers := make(chan map[string]any, 16)
	a, e := mcp.NewClient(&mcp.Implementation{Name: "recovery-A-protocol-test", Version: "1"}, nil).Connect(f.ctx, observeTransport{mcpagent.NewTransport(socket), offers}, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { a.Close() })
	select {
	case <-offers:
	case <-f.ctx.Done():
		t.Fatal("no recovery notification")
	}
	resumed := tool(t, f.ctx, a, "task_resume", map[string]any{"task_id": id, "previous_assignment_id": assignment})
	newAssignment := resumed["assignment_id"].(string)
	if newAssignment == assignment {
		t.Fatal("assignment was not fenced")
	}
	again := tool(t, f.ctx, a, "bash", map[string]any{"task_id": id, "assignment_id": newAssignment, "operation_key": "before-backup", "command": command, "wait_ms": 10000})
	if again["command"].(map[string]any)["command_id"] != cmdID {
		t.Fatal("recovery changed command identity")
	}
	tool(t, f.ctx, a, "task_complete", map[string]any{"task_id": id, "assignment_id": newAssignment, "outcome": "succeeded", "summary": "Recovered without repeating execution."})
	for _, name := range []string{"recovery-proof.txt", "post-backup.txt"} {
		data, e := os.ReadFile(filepath.Join(f.root, name))
		if e != nil || strings.Count(string(data), "\n") != 1 {
			t.Fatalf("recovery repeated side effects: %s %q %v", name, data, e)
		}
	}
	if e = s.View(f.ctx, func(tx *storage.Tx) error { _, e := tx.Command(orphanID); return e }); e == nil {
		t.Fatal("orphan was recreated as an executable command")
	}
}
