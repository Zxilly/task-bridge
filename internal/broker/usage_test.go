package broker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
)

func reportFixtureUsage(t *testing.T, f *brokerFixture, usage model.AccountUsage) {
	t.Helper()
	_, err := f.b.CallAgent(f.ctx, f.claude, f.connectionID, "agent_usage_update", model.JSON(model.AgentUsageUpdateArgs{Usage: usage}))
	if err != nil {
		t.Fatal(err)
	}
}

func queryFixtureUsage(t *testing.T, f *brokerFixture, agentID string) model.AgentsUsageResult {
	t.Helper()
	result, err := f.b.CallClient(f.ctx, f.peer, "agents_usage", model.JSON(model.AgentsUsageArgs{WorkspaceID: "workspace-1", AgentID: agentID}))
	if err != nil {
		t.Fatal(err)
	}
	return result.(model.AgentsUsageResult)
}

func fixtureUsage(now time.Time) model.AccountUsage {
	return model.AccountUsage{Source: model.AccountUsageSource, Status: "allowed", ObservedAt: now.Unix(), Windows: map[string]model.UsageWindow{
		"five_hour": {UsedPercentage: new(24.0), ResetsAt: new(now.Unix() + 3600), ObservedAt: now.Unix()},
		"seven_day": {UsedPercentage: new(0.0), ResetsAt: new(now.Unix() + 86400), ObservedAt: now.Unix()},
	}}
}

func TestAgentsUsageQueriesWithoutTaskAndRecomputesFreshness(t *testing.T) {
	f := newFixture(t)
	initial := queryFixtureUsage(t, f, "")
	if len(initial.Agents) != 1 || initial.Agents[0].Usage.Available || !initial.Agents[0].Usage.Stale {
		t.Fatalf("initial usage: %+v", initial)
	}
	u := fixtureUsage(time.Now())
	u.Source = model.AccountUsageCommandSource
	u.Reason = "untrusted reporter explanation"
	reportFixtureUsage(t, f, u)
	result := queryFixtureUsage(t, f, "claude-1")
	if len(result.Agents) != 1 {
		t.Fatalf("agent filter: %+v", result)
	}
	agent := result.Agents[0]
	if agent.Usage.Source != model.AccountUsageCommandSource {
		t.Fatal("broker lost the local-command quota source")
	}
	if !agent.Usage.Available || agent.Usage.Stale || agent.ReceivedAt == 0 || *agent.Usage.Windows["five_hour"].UsedPercentage != 24 || *agent.Usage.Windows["seven_day"].UsedPercentage != 0 || agent.Usage.Reason != "" {
		t.Fatalf("reported usage: %+v", result)
	}
	*agent.Usage.Windows["five_hour"].UsedPercentage = 99
	if *queryFixtureUsage(t, f, "").Agents[0].Usage.Windows["five_hour"].UsedPercentage != 24 {
		t.Fatal("query result mutated broker cache")
	}
	if len(queryFixtureUsage(t, f, "absent-agent").Agents) != 0 {
		t.Fatal("agent filter leaked another agent")
	}
	u = fixtureUsage(time.Now().Add(-301 * time.Second))
	u.Available, u.Stale = false, false
	staleFixture := newFixture(t)
	reportFixtureUsage(t, staleFixture, u)
	if got := queryFixtureUsage(t, staleFixture, "").Agents[0].Usage; !got.Available || !got.Stale || !got.Windows["five_hour"].Stale {
		t.Fatalf("old data freshness trusted reporter: %+v", got)
	}
	u = fixtureUsage(time.Now())
	window := u.Windows["five_hour"]
	window.ResetsAt = new(time.Now().Unix() - 1)
	u.Windows["five_hour"] = window
	reportFixtureUsage(t, f, u)
	if got := queryFixtureUsage(t, f, "").Agents[0].Usage; !got.Stale || !got.Windows["five_hour"].Stale {
		t.Fatalf("reset window remained current: %+v", got)
	}
}

func TestAgentUsageDoesNotReplaceFreshRefreshWithDelayedTaskEvent(t *testing.T) {
	f := newFixture(t)
	fresh := fixtureUsage(time.Now())
	fresh.Source = model.AccountUsageCommandSource
	reportFixtureUsage(t, f, fresh)
	delayed := fixtureUsage(time.Now().Add(-time.Minute))
	*delayed.Windows["five_hour"].UsedPercentage = 1
	reportFixtureUsage(t, f, delayed)
	got := queryFixtureUsage(t, f, "").Agents[0].Usage
	if got.Source != model.AccountUsageCommandSource || got.ObservedAt != fresh.ObservedAt || *got.Windows["five_hour"].UsedPercentage != 24 {
		t.Fatalf("delayed event rolled back a newer local-command refresh: %+v", got)
	}
}

func TestAgentUsageFencesConnectionsAndRejectsInvalidReports(t *testing.T) {
	f := newFixture(t)
	u := fixtureUsage(time.Now())
	raw := model.JSON(model.AgentUsageUpdateArgs{Usage: u})
	_, err := f.b.CallAgent(f.ctx, f.claude, "old-connection", "agent_usage_update", raw)
	requireCode(t, err, "stale_connection")
	_, err = f.b.CallAgent(f.ctx, f.executor, f.connectionID, "agent_usage_update", raw)
	requireCode(t, err, "forbidden")
	for _, edit := range []func(*model.AccountUsage){
		func(u *model.AccountUsage) { u.Source = "other" },
		func(u *model.AccountUsage) { u.ObservedAt = time.Now().Unix() + 3600 },
		func(u *model.AccountUsage) { u.Status = "unknown-status" },
		func(u *model.AccountUsage) { u.Windows["other"] = u.Windows["five_hour"] },
		func(u *model.AccountUsage) { *u.Windows["five_hour"].UsedPercentage = 101 },
	} {
		invalid := fixtureUsage(time.Now())
		edit(&invalid)
		_, err := f.b.CallAgent(f.ctx, f.claude, f.connectionID, "agent_usage_update", model.JSON(model.AgentUsageUpdateArgs{Usage: invalid}))
		requireCode(t, err, "invalid_arguments")
	}
	if queryFixtureUsage(t, f, "").Agents[0].Usage.Available {
		t.Fatal("rejected report changed usage")
	}
	reportFixtureUsage(t, f, u)
	f.b.AgentDisconnected(f.claude, f.connectionID)
	if len(queryFixtureUsage(t, f, "").Agents) != 0 {
		t.Fatal("disconnected agent still advertised")
	}
	if err := f.b.AgentConnected(f.ctx, f.claude, "new-connection", func(context.Context, model.ChannelNotification) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if queryFixtureUsage(t, f, "").Agents[0].Usage.Available {
		t.Fatal("reconnected agent inherited an earlier connection's usage")
	}
	_, err = f.b.CallAgent(f.ctx, f.claude, f.connectionID, "agent_usage_update", raw)
	requireCode(t, err, "stale_connection")
}

func TestAgentsUsageEnforcesWorkspaceAndLiveClient(t *testing.T) {
	f := newFixture(t)
	if err := f.b.CreateWorkspace(f.ctx, model.Workspace{ID: "other", ExecutorID: "other-executor"}); err != nil {
		t.Fatal(err)
	}
	_, err := f.b.CallClient(f.ctx, f.peer, "agents_usage", model.JSON(model.AgentsUsageArgs{WorkspaceID: "other"}))
	requireCode(t, err, "forbidden")
	f.peer.Workspaces = map[string]bool{"other": true}
	_, err = f.b.CallClient(f.ctx, f.peer, "agents_usage", model.JSON(model.AgentsUsageArgs{WorkspaceID: "workspace-1"}))
	requireCode(t, err, "forbidden")
	f.peer.Workspaces = nil
	f.b.DisconnectClient(f.peer)
	_, err = f.b.CallClient(f.ctx, f.peer, "agents_usage", model.JSON(model.AgentsUsageArgs{WorkspaceID: "workspace-1"}))
	requireCode(t, err, "stale_connection")
}

func TestAgentsUsageBoundsAgentEnumeration(t *testing.T) {
	f := newFixture(t)
	for i := range 10 {
		id := fmt.Sprintf("extra-%d", i)
		p, _, err := f.b.CreateDevice(f.ctx, model.Principal{Kind: "claude", AgentID: id, WorkspaceID: "workspace-1"})
		if err != nil {
			t.Fatal(err)
		}
		if err := f.b.AgentConnected(f.ctx, p, id, func(context.Context, model.ChannelNotification) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	_, err := f.b.CallClient(f.ctx, f.peer, "agents_usage", model.JSON(model.AgentsUsageArgs{WorkspaceID: "workspace-1"}))
	requireCode(t, err, "result_too_large")
	if result := queryFixtureUsage(t, f, "claude-1"); len(result.Agents) != 1 {
		t.Fatalf("filtered query should fit: %+v", result)
	}
}
