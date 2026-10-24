package agent

import (
	"context"
	"testing"

	"github.com/Zxilly/task-bridge/internal/model"
)

func TestWorkerSubmissionToolsCarryLaunchOptions(t *testing.T) {
	for _, method := range []string{"tasks_submit", "tasks_run"} {
		t.Run(method, func(t *testing.T) {
			f := setupWorkerMCPResume(t)
			sessions := make(chan Session, 2)
			f.start(t, func(ctx context.Context, session Session) error {
				if err := emitWorkerMCPSession(ctx, sessions, session); err != nil {
					return err
				}
				relay, err := taskClient(ctx, session)
				if err != nil {
					return err
				}
				defer relay.Close()
				_, err = workerRelayTool[model.Task](ctx, relay, "task_complete", map[string]string{
					"task_id": session.Task.TaskID, "assignment_id": session.Task.AssignmentID,
					"outcome": "succeeded", "summary": "launch options received",
				})
				return err
			})
			for i, options := range []struct{ modelName, effort string }{{"opus", "xhigh"}, {}} {
				key := "with-options"
				if i == 1 {
					key = "without-options"
				}
				args := map[string]any{
					"workspace_id": "workspace", "prompt": "Inspect the configured task options.",
					"idempotency_key": key, "model": options.modelName, "effort": options.effort,
				}
				if method == "tasks_run" {
					args["wait_ms"] = 1
				}
				workerMCPTool[any](t, f, method, args)
				session := f.nextSession(t, sessions)
				if session.Model != options.modelName || session.Effort != options.effort {
					t.Fatalf("MCP to worker lost launch options: model=%q effort=%q", session.Model, session.Effort)
				}
				final := f.waitTask(t, session.Task.TaskID, "completed task", func(task model.Task) bool { return task.State == "succeeded" })
				if final.Model != options.modelName || final.Effort != options.effort {
					t.Fatalf("stored task lost launch options: %+v", final)
				}
			}
		})
	}
}
