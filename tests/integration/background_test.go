package integration

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestBackgroundCommandLifecycle(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		name := "explicit"
		if automatic {
			name = "automatic"
			if runtime.GOOS == "windows" {
				continue
			}
		}
		t.Run(name, func(t *testing.T) {
			f := setup(t)
			tool(t, f.ctx, f.host, "tasks_submit", map[string]any{"workspace_id": "demo", "prompt": "Verify managed background execution.", "idempotency_key": "background"})
			id, assignment := accept(t, f)
			command := "printf early; while [ ! -f release ]; do sleep 0.05; done; printf late"
			if automatic {
				command = "(" + command + ") & printf shell-exit"
			}
			args := map[string]any{"task_id": id, "assignment_id": assignment, "operation_key": "background", "command": command, "background": !automatic, "timeout_ms": 15000, "wait_ms": 1}
			begin := time.Now()
			first := tool(t, f.ctx, f.a, "bash", args)
			if time.Since(begin) > 2*time.Second || first["pending"] != true {
				t.Fatalf("background submission blocked: %+v", first)
			}
			cid := first["command"].(map[string]any)["command_id"]
			for {
				status := tool(t, f.ctx, f.a, "background_get", map[string]any{"task_id": id, "command_id": cid})
				c := status["command"].(map[string]any)
				if c["background"] == true && c["state"] == "running" {
					if automatic && c["shell_exit_code"] != float64(0) {
						t.Fatalf("shell exit lost: %+v", c)
					}
					break
				}
				select {
				case <-f.ctx.Done():
					t.Fatal(f.ctx.Err())
				case <-time.After(50 * time.Millisecond):
				}
			}
			// Another foreground command can run while the tracked background is live.
			tool(t, f.ctx, f.a, "bash", map[string]any{"task_id": id, "assignment_id": assignment, "operation_key": "release", "command": "printf go > release; printf side", "wait_ms": 10000})
			done := tool(t, f.ctx, f.a, "background_get", map[string]any{"task_id": id, "command_id": cid, "wait_ms": 10000})
			c := done["command"].(map[string]any)
			if done["pending"] != false || c["state"] != "exited" || c["exit_code"] != float64(0) || !strings.Contains(done["stdout"].(string), "early") || !strings.Contains(done["stdout"].(string), "late") {
				t.Fatalf("bad background result: %+v", done)
			}
			retry := tool(t, f.ctx, f.a, "bash", args)
			if retry["command"].(map[string]any)["command_id"] != cid {
				t.Fatal("background command duplicated")
			}
			tool(t, f.ctx, f.a, "task_complete", map[string]any{"task_id": id, "assignment_id": assignment, "outcome": "succeeded", "summary": "Managed background completed with preserved output."})
		})
	}
}

func TestBackgroundCommandCanBeCancelled(t *testing.T) {
	f := setup(t)
	tool(t, f.ctx, f.host, "tasks_submit", map[string]any{"workspace_id": "demo", "prompt": "Verify background cancellation.", "idempotency_key": "background-cancel"})
	id, assignment := accept(t, f)
	r := tool(t, f.ctx, f.a, "bash", map[string]any{"task_id": id, "assignment_id": assignment, "operation_key": "background", "command": "sleep 30", "background": true, "timeout_ms": 60000})
	cid := r["command"].(map[string]any)["command_id"]
	for {
		s := tool(t, f.ctx, f.a, "background_get", map[string]any{"task_id": id, "command_id": cid})
		if s["command"].(map[string]any)["state"] == "running" {
			break
		}
		select {
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		case <-time.After(30 * time.Millisecond):
		}
	}
	tool(t, f.ctx, f.a, "command_cancel", map[string]any{"task_id": id, "command_id": cid})
	done := tool(t, f.ctx, f.a, "background_get", map[string]any{"task_id": id, "command_id": cid, "wait_ms": 10000})
	if done["command"].(map[string]any)["state"] != "cancelled" {
		t.Fatalf("cancel failed: %+v", done)
	}
}
