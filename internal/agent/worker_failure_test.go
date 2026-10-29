package agent

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestWorkerReportsCLIRejectionWithoutDisconnectingOrRetrying(t *testing.T) {
	for _, mode := range []string{"auth-error", "rate-error"} {
		t.Run(mode, func(t *testing.T) {
			f := setupWorker(t)
			launcher, _ := launcherHelper(t, mode)
			first := f.submit(t, "cli-failure", "do not retry this task")
			var launches atomic.Int32
			f.worker.launcher = launcherFunc(func(ctx context.Context, session Session) error {
				launches.Add(1)
				if session.Task.TaskID == first.ID {
					return launcher.Launch(ctx, session)
				}
				host, err := taskClient(ctx, session)
				if err != nil {
					return err
				}
				defer host.Close()
				result, err := host.CallTool(ctx, &mcp.CallToolParams{Name: "task_complete", Arguments: map[string]any{"task_id": session.Task.TaskID, "assignment_id": session.Task.AssignmentID, "outcome": "succeeded", "summary": "next task succeeded on the same connection"}})
				if err != nil {
					return err
				}
				_, err = decodeTool[model.Task](result)
				return err
			})
			run := startFixtureController(t, f)
			interrupted := waitWorkerTask(t, f, first.ID, "interrupted")
			var wire struct {
				Error *model.Error `json:"error"`
			}
			if err := json.Unmarshal(model.JSON(interrupted), &wire); err != nil {
				t.Fatal(err)
			}
			want := "claude_authentication_failed"
			if mode == "rate-error" {
				want = "claude_rate_limited"
			}
			if wire.Error == nil || wire.Error.Code != want {
				t.Fatalf("CLI rejection became an unexplained disconnect: %+v; error=%+v", interrupted, wire.Error)
			}
			if strings.Contains(string(model.JSON(interrupted)), "private") {
				t.Fatal("raw CLI content crossed into the public task")
			}
			time.Sleep(1200 * time.Millisecond)
			run.requireAlive(t)
			if launches.Load() != 1 {
				t.Fatal("failed task was automatically retried")
			}
			// An interrupted task retains its workspace reservation until B
			// explicitly cancels or resumes it, even when no command ran.
			if _, err := f.broker.CallClient(f.ctx, f.peer, "tasks_cancel", model.JSON(map[string]any{"task_id": first.ID, "reason": "test finished inspecting the failed attempt"})); err != nil {
				t.Fatal(err)
			}
			waitWorkerTask(t, f, first.ID, "cancelled")
			next := f.submit(t, "after-cli-failure", "complete a separate task")
			finished := waitWorkerTask(t, f, next.ID, "succeeded")
			if finished.ConnectionID != interrupted.ConnectionID {
				t.Fatal("child failure disconnected the controller")
			}
		})
	}
}

func TestBReadsDurableCLIAuthenticationFailureThroughMCP(t *testing.T) {
	f := setupWorkerMCPResume(t)
	launcher, _ := launcherHelper(t, "auth-error")
	f.start(t, launcher.Launch)
	f.waitConnected(t)
	task := workerMCPTool[model.Task](t, f, "tasks_submit", model.TaskSubmitArgs{WorkspaceID: "workspace", IdempotencyKey: "authentication-report", Prompt: "Reply only with a task completion.", DeadlineMS: 30000})
	interrupted := f.waitTask(t, task.ID, "public authentication failure", func(task model.Task) bool { return task.State == "interrupted" })
	if interrupted.Error == nil || interrupted.Error.Code != "claude_authentication_failed" || !strings.Contains(interrupted.Error.Message, "403") {
		t.Fatalf("B lost the process diagnosis: %+v", interrupted)
	}
	events := workerMCPTool[model.EventList](t, f, "tasks_events", map[string]any{"task_id": task.ID, "after_seq": 0, "limit": 10})
	found := false
	for _, event := range events {
		if event.Type == "agent.disconnected" {
			t.Fatal("CLI rejection was still reported as a transport disconnect")
		}
		if event.Type == "task.interrupted" {
			found = true
		}
	}
	if !found {
		t.Fatal("B did not receive the durable interruption event")
	}
}
