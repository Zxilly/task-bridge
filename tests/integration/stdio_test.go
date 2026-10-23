package integration

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/client"
	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestBuiltClientStdioAndHostEOF(t *testing.T) {
	f := setup(t)
	f.stopB()
	dir := t.TempDir()
	binary := filepath.Join(dir, "task-bridge-client")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	build := exec.CommandContext(f.ctx, "go", "build", "-o", binary, "./cmd/task-bridge-client")
	build.Dir = filepath.Join("..", "..")
	if output, e := build.CombinedOutput(); e != nil {
		t.Fatalf("build actual client CLI: %v\n%s", e, output)
	}
	configPath := filepath.Join(dir, "client.json")
	if e := os.WriteFile(configPath, model.JSON(f.cfg), 0600); e != nil {
		t.Fatal(e)
	}
	command := exec.Command(binary, "mcp", "--config", configPath)
	command.Stderr = os.Stderr
	host, e := mcp.NewClient(&mcp.Implementation{Name: "official-stdio-host", Version: "1"}, nil).Connect(f.ctx, &mcp.CommandTransport{Command: command, TerminateDuration: 3 * time.Second}, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { host.Close() })
	listed, e := host.ListTools(f.ctx, nil)
	if e != nil || len(listed.Tools) != 7 {
		t.Fatalf("stdio tool listing: %v %+v", e, listed)
	}
	submit := tool(t, f.ctx, host, "tasks_submit", map[string]any{"workspace_id": "demo", "prompt": "Verify real stdio transport.", "idempotency_key": "stdio-1"})
	id, assignment := accept(t, f)
	if id != submit["task_id"] {
		t.Fatal("stdio submitted wrong task")
	}
	r := tool(t, f.ctx, f.a, "bash", map[string]any{"task_id": id, "assignment_id": assignment, "operation_key": "stdio-exec", "command": "printf 'stdout-marker'; printf 'stderr-marker' >&2", "wait_ms": 10000})
	if r["stdout"] != "stdout-marker" || r["stderr"] != "stderr-marker" {
		t.Fatalf("command output corrupted stdio protocol: %+v", r)
	}
	tool(t, f.ctx, f.a, "task_complete", map[string]any{"task_id": id, "assignment_id": assignment, "outcome": "succeeded", "summary": "Actual CLI stdio verified."})
	waited := tool(t, f.ctx, host, "tasks_wait", map[string]any{"task_id": id, "wait_ms": 10000})
	if waited["pending"] != false {
		t.Fatal("stdio result missing")
	}
	tool(t, f.ctx, host, "tasks_submit", map[string]any{"workspace_id": "demo", "prompt": "Stop local work when the MCP host closes.", "idempotency_key": "stdio-eof"})
	id, assignment = accept(t, f)
	running := tool(t, f.ctx, f.a, "bash", map[string]any{"task_id": id, "assignment_id": assignment, "operation_key": "eof", "command": "sleep 1; printf 'must-not-run' > eof-late.txt", "wait_ms": 1})
	cmdID := running["command"].(map[string]any)["command_id"].(string)
	for {
		state := tool(t, f.ctx, f.a, "command_get", map[string]any{"task_id": id, "command_id": cmdID})["command"].(map[string]any)["state"]
		if state == "running" {
			break
		}
		select {
		case <-f.ctx.Done():
			t.Fatal("child command did not start")
		case <-time.After(10 * time.Millisecond):
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- host.Close() }()
	select {
	case e := <-closed:
		if e != nil {
			t.Fatal("closing stdio host:", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client did not exit after stdin EOF")
	}
	select {
	case <-time.After(1200 * time.Millisecond):
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	if _, e = os.Stat(filepath.Join(f.root, "eof-late.txt")); !os.IsNotExist(e) {
		t.Fatal("stdio EOF left local command running")
	}
	j, e := client.OpenJournal(f.cfg.JournalPath)
	if e != nil {
		t.Fatal("exited client retained its journal lock:", e)
	}
	defer j.Close()
	events, e := j.Pending(128)
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, m := range events {
		var result model.CommandEvent
		if m.Type == "command.finished" && json.Unmarshal(m.Payload, &result) == nil && result.CommandID == cmdID {
			found = true
			if result.State != "cancelled" && result.State != "not_started" {
				t.Fatalf("EOF did not persist a stopped command: %+v", result)
			}
		}
	}
	if !found {
		t.Log("final event was already acknowledged before shutdown")
	}
}

func TestClientProcessCrashReportsUnknownAndNeverReexecutes(t *testing.T) {
	f := setup(t)
	f.stopB()
	dir := t.TempDir()
	binary := filepath.Join(dir, "task-bridge-client")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	build := exec.CommandContext(f.ctx, "go", "build", "-o", binary, "./cmd/task-bridge-client")
	build.Dir = filepath.Join("..", "..")
	if data, e := build.CombinedOutput(); e != nil {
		t.Fatalf("build client: %v %s", e, data)
	}
	config := filepath.Join(dir, "client.json")
	if e := os.WriteFile(config, model.JSON(f.cfg), 0600); e != nil {
		t.Fatal(e)
	}
	connect := func() (*mcp.ClientSession, *exec.Cmd) {
		cmd := exec.Command(binary, "mcp", "--config", config)
		cmd.Stderr = os.Stderr
		host, e := mcp.NewClient(&mcp.Implementation{Name: "crash-test-stdio-host", Version: "1"}, nil).Connect(f.ctx, &mcp.CommandTransport{Command: cmd, TerminateDuration: 2 * time.Second}, nil)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { host.Close() })
		return host, cmd
	}
	host, process := connect()
	tool(t, f.ctx, host, "tasks_submit", map[string]any{"workspace_id": "demo", "prompt": "Do not repeat an execution with an unknown result.", "idempotency_key": "hard-crash"})
	id, assignment := accept(t, f)
	release := filepath.Join(f.root, "crash-release.txt")
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0600) })
	r := tool(t, f.ctx, f.a, "bash", map[string]any{"task_id": id, "assignment_id": assignment, "operation_key": "unknown-on-crash", "command": "printf 'once\\n' >> crash-proof.txt; while [ ! -f crash-release.txt ]; do sleep 0.1; done", "wait_ms": 1})
	cmdID := r["command"].(map[string]any)["command_id"].(string)
	for {
		v := tool(t, f.ctx, f.a, "command_get", map[string]any{"task_id": id, "command_id": cmdID})
		if v["command"].(map[string]any)["state"] == "running" {
			break
		}
		select {
		case <-f.ctx.Done():
			t.Fatal("command never started")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Crash after an observable effect but before a terminal result. Windows
	// closes the execution Job when the client dies, so the effect must precede
	// the crash; recovery must still not infer that unreported work had no effect.
	effectDeadline := time.Now().Add(4 * time.Second)
	for {
		data, err := os.ReadFile(filepath.Join(f.root, "crash-proof.txt"))
		if err == nil && string(data) == "once\n" {
			break
		}
		if time.Now().After(effectDeadline) {
			t.Fatalf("test command did not record its controlled effect: %q %v", data, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Kill only the known test client process.
	if e := process.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	// Release a surviving Unix child only after its reporting process is dead.
	if e := os.WriteFile(release, nil, 0600); e != nil {
		t.Fatal(e)
	}
	_ = host.Close()
	restarted, _ := connect()
	for {
		value := tool(t, f.ctx, restarted, "tasks_get", map[string]any{"task_id": id})
		if value["task"].(map[string]any)["state"] == "interrupted" {
			break
		}
		select {
		case <-f.ctx.Done():
			t.Fatal("crashed execution was not marked interrupted")
		case <-time.After(20 * time.Millisecond):
		}
	}
	v := tool(t, f.ctx, f.a, "command_get", map[string]any{"task_id": id, "command_id": cmdID})
	if v["command"].(map[string]any)["state"] != "unknown" {
		t.Fatal("process crash was presented as a known execution outcome")
	}
	resume, e := f.a.CallTool(f.ctx, &mcp.CallToolParams{Name: "task_resume", Arguments: map[string]any{"task_id": id, "previous_assignment_id": assignment}})
	if e != nil || !resume.IsError {
		t.Fatalf("unknown command was resumable: %+v %v", resume, e)
	}
	deadline := time.Now().Add(4 * time.Second)
	for {
		data, e := os.ReadFile(filepath.Join(f.root, "crash-proof.txt"))
		if e == nil {
			if string(data) != "once\n" {
				t.Fatalf("crashed command was repeated: %q", data)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("test child did not finish its single controlled effect", e)
		}
		time.Sleep(20 * time.Millisecond)
	}
	after := tool(t, f.ctx, restarted, "tasks_get", map[string]any{"task_id": id})
	if after["task"].(map[string]any)["state"] != "interrupted" {
		t.Fatal("reconnect automatically resumed an unknown execution")
	}
}
