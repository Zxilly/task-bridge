package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
)

func TestIdleWorkerRefreshesUsageAfterConnectAndReconnectWithoutTask(t *testing.T) {
	f := setupWorkerMCPResume(t)
	_, _ = launcherHelper(t, "usage-query")
	f.worker.usageReader = CommandLauncher{Path: f.worker.cfg.ClaudePath}
	f.worker.launcher = launcherFunc(func(context.Context, Session) error {
		t.Error("quota refresh started a model task")
		return errors.New("no task should be launched")
	})
	for attempt := range 2 {
		func() {
			ctx, cancel := context.WithCancel(f.ctx)
			done := make(chan error, 1)
			go func() { done <- f.worker.Run(ctx) }()
			defer func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Error("idle worker did not stop")
				}
			}()
			deadline := time.Now().Add(5 * time.Second)
			for {
				result := workerMCPTool[model.AgentsUsageResult](t, f, "agents_usage", model.AgentsUsageArgs{WorkspaceID: "workspace"})
				if len(result.Agents) == 1 && result.Agents[0].Usage.Available && !result.Agents[0].Usage.Stale {
					usage := result.Agents[0].Usage
					if usage.Source != "claude_code_usage_command" || *usage.Windows["five_hour"].UsedPercentage != 24 || *usage.Windows["seven_day"].UsedPercentage != 38 {
						t.Fatalf("wrong idle account data: %+v", usage)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("attempt %d: connected idle A never supplied usable quota without a task", attempt+1)
				}
				time.Sleep(10 * time.Millisecond)
			}
			listed := workerMCPTool[model.TaskListResult](t, f, "tasks_list", model.TaskListArgs{WorkspaceID: "workspace"})
			if len(listed.Tasks) != 0 {
				t.Fatal("quota refresh created a broker task")
			}
		}()
		deadline := time.Now().Add(2 * time.Second)
		for {
			result := workerMCPTool[model.AgentsUsageResult](t, f, "agents_usage", model.AgentsUsageArgs{WorkspaceID: "workspace"})
			if len(result.Agents) == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("old agent connection was not removed")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
