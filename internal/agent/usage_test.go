package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
)

func TestAccountUsageWindowsAndFreshness(t *testing.T) {
	u := &accountUsage{}
	now := time.Unix(1000, 0)
	if s := u.snapshot(now); s.Available || !s.Stale || s.Reason == "" || len(s.Windows) != 0 {
		t.Fatalf("unknown usage claimed available: %+v", s)
	}
	u.observe([]byte(`{"type":"rate_limit_event","session_id":"session","rate_limit_info":{"status":"allowed","unifiedWindows":{"five_hour":{"utilization":0,"resetsAt":1100},"seven_day":{"utilization":0.42,"resetsAt":5000}}}}`), "session", now)
	s := u.snapshot(now)
	if !s.Available || s.Stale || len(s.Windows) != 2 || *s.Windows["five_hour"].UsedPercentage != 0 || *s.Windows["seven_day"].UsedPercentage != 42 {
		t.Fatalf("two window snapshot: %+v", s)
	}
	if s := u.snapshot(time.Unix(1100, 0)); !s.Stale || !s.Windows["five_hour"].Stale || s.Windows["seven_day"].Stale {
		t.Fatalf("expired window claimed fresh: %+v", s)
	}
	if s := u.snapshot(time.Unix(1301, 0)); !s.Windows["seven_day"].Stale {
		t.Fatalf("old observation claimed fresh: %+v", s)
	}
	// A status-only observation must not freshen earlier windows. A subsequent
	// reset-only observation must not turn unknown utilization into zero.
	u.observe([]byte(`{"type":"rate_limit_event","session_id":"session","rate_limit_info":{"status":"allowed_warning"}}`), "session", time.Unix(1400, 0))
	if s := u.snapshot(time.Unix(1400, 0)); !s.Stale || s.Windows["seven_day"].ObservedAt != 1000 {
		t.Fatalf("status refreshed absent window: %+v", s)
	}
	u.observe([]byte(`{"type":"rate_limit_event","session_id":"session","rate_limit_info":{"status":"rejected","rateLimitType":"five_hour","resetsAt":2500}}`), "session", time.Unix(1500, 0))
	s = u.snapshot(time.Unix(1500, 0))
	if s.Windows["five_hour"].UsedPercentage != nil || *s.Windows["five_hour"].ResetsAt != 2500 || s.Status != "rejected" {
		t.Fatalf("missing utilization was invented: %+v", s)
	}
}

func TestAccountUsageIgnoresInvalidAndOtherSessionEvents(t *testing.T) {
	for _, line := range []string{
		`not-json`,
		`{"type":"assistant","session_id":"session","rate_limit_info":{"status":"allowed","rateLimitType":"five_hour","utilization":0.2}}`,
		`{"type":"rate_limit_event","session_id":"other","rate_limit_info":{"status":"allowed","rateLimitType":"five_hour","utilization":0.2}}`,
		`{"type":"rate_limit_event","session_id":"session","rate_limit_info":{"status":"new-status","rateLimitType":"five_hour","utilization":0.2}}`,
		`{"type":"rate_limit_event","session_id":"session","rate_limit_info":{"status":"allowed","rateLimitType":"five_hour","utilization":-1}}`,
		`{"type":"rate_limit_event","session_id":"session","rate_limit_info":{"status":"allowed","rateLimitType":"five_hour","utilization":2}}`,
		`{"type":"rate_limit_event","session_id":"session","rate_limit_info":{"status":"allowed","rateLimitType":"five_hour","utilization":0.2,"resetsAt":-1}}`,
		`{"type":"rate_limit_event","session_id":"session","rate_limit_info":{"status":"allowed","rateLimitType":"unknown","utilization":0.2}}`,
	} {
		u := &accountUsage{}
		u.observe([]byte(line), "session", time.Now())
		if u.snapshot(time.Now()).Available {
			t.Errorf("invalid event accepted: %s", line)
		}
	}
}

func TestUsageStreamDrainsLargeLinesAndPartialWrites(t *testing.T) {
	u := &accountUsage{}
	s := &usageStream{diagnostic: &launchDiagnostic{}, usage: u, sessionID: "session"}
	_, _ = s.Write([]byte(strings.Repeat("x", maxUsageEventBytes+1)))
	_, _ = s.Write([]byte("\nmalformed\n"))
	line := `{"type":"rate_limit_event","session_id":"session","rate_limit_info":{"status":"allowed_warning","rateLimitType":"seven_day_sonnet","utilization":0.9,"resetsAt":4102444800}}`
	for i := range len(line) {
		_, _ = s.Write([]byte(line[i : i+1]))
	}
	s.finish()
	snapshot := u.snapshot(time.Now())
	if len(snapshot.Windows) != 1 || *snapshot.Windows["seven_day_sonnet"].UsedPercentage != 90 || len(s.diagnostic.data) > maxLaunchDiagnosticBytes {
		t.Fatalf("stream lost a valid event: %+v", snapshot)
	}
}

func TestRunningLauncherReportsUsageToBWithoutClaudeInvolvement(t *testing.T) {
	f := setupWorkerMCPResume(t)
	launcher, _ := launcherHelper(t, "usage")
	f.start(t, func(ctx context.Context, session Session) error {
		if strings.Contains(session.Prompt, "account_usage") || strings.Contains(session.Prompt, "account five-hour") {
			return errors.New("Claude was instructed to manage account usage")
		}
		relay, err := taskClient(ctx, session)
		if err != nil {
			return err
		}
		list, err := relay.ListTools(ctx, nil)
		_ = relay.Close()
		if err != nil {
			return err
		}
		for _, tool := range list.Tools {
			if tool.Name == "account_usage" || tool.Name == "agent_usage_update" {
				return errors.New("account usage tool exposed to Claude")
			}
		}
		return launcher.Launch(ctx, session)
	})
	f.waitConnected(t)
	read := func() model.AgentsUsageResult {
		return workerMCPTool[model.AgentsUsageResult](t, f, "agents_usage", model.AgentsUsageArgs{WorkspaceID: "workspace"})
	}
	initial := read()
	if len(initial.Agents) != 1 || initial.Agents[0].Usage.Available || !initial.Agents[0].Usage.Stale {
		t.Fatalf("idle agent usage should start unknown: %+v", initial)
	}
	tasks := workerMCPTool[model.TaskListResult](t, f, "tasks_list", model.TaskListArgs{WorkspaceID: "workspace"})
	if len(tasks.Tasks) != 0 {
		t.Fatal("usage query created a task")
	}
	task := workerMCPTool[model.Task](t, f, "tasks_submit", model.TaskSubmitArgs{WorkspaceID: "workspace", Prompt: "Check passive account usage reporting.", IdempotencyKey: "usage", DeadlineMS: 30000})
	deadline := time.Now().Add(10 * time.Second)
	for {
		result := read()
		if len(result.Agents) == 1 && result.Agents[0].Usage.Available {
			agent := result.Agents[0]
			snapshot := agent.Usage
			if agent.ReceivedAt == 0 || snapshot.Status != "allowed" || snapshot.Stale || len(snapshot.Windows) != 2 || *snapshot.Windows["five_hour"].UsedPercentage != 24 || *snapshot.Windows["seven_day"].UsedPercentage != 13 {
				t.Fatalf("wrong CLI usage at B: %+v", result)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("running CLI usage never reached B")
		}
		time.Sleep(20 * time.Millisecond)
	}
	workerMCPTool[model.Task](t, f, "tasks_cancel", model.TaskCancelArgs{TaskID: task.ID, Reason: "Usage reporting verified."})
	f.waitTask(t, task.ID, "cancelled helper task", func(task model.Task) bool { return task.Terminal() })
	if result := read(); len(result.Agents) != 1 || !result.Agents[0].Usage.Available {
		t.Fatalf("last usage disappeared after task completion: %+v", result)
	}
}
