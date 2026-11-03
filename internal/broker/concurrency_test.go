package broker

import (
	"context"
	"testing"

	"github.com/Zxilly/task-bridge/internal/model"
)

func TestSameWorkspaceSchedulesConcurrentTasksWithinAgentCapacity(t *testing.T) {
	f := newFixture(t)
	if _, err := f.b.CallAgent(f.ctx, f.claude, f.connectionID, "agent_recovery_configure", model.JSON(map[string]any{"auto_resume": true, "max_concurrent_tasks": 2})); err != nil {
		t.Fatal(err)
	}
	first := f.submit("concurrent-first", "first task")
	second := f.submit("concurrent-second", "second task")
	third := f.submit("concurrent-third", "third task")
	f.b.tick(f.ctx)
	// Submissions can share a millisecond; the scheduler breaks timestamp
	// ties by UUID. Check capacity without assuming submission-call order.
	var offered []model.Task
	var queued []model.Task
	for _, candidate := range []model.Task{first, second, third} {
		task := f.task(candidate.ID)
		switch task.State {
		case "offered":
			offered = append(offered, task)
		case "queued":
			queued = append(queued, task)
		default:
			t.Fatalf("unexpected task state: %+v", task)
		}
	}
	if len(offered) != 2 || len(queued) != 1 {
		t.Fatalf("capacity mismatch: offered=%d queued=%d", len(offered), len(queued))
	}
	first, second, third = offered[0], offered[1], queued[0]
	for _, id := range []string{first.ID, second.ID} {
		task := f.task(id)
		if task.State != "offered" {
			t.Fatalf("task %s state=%s", id, task.State)
		}
		if _, err := f.agent("task_accept", Args{TaskID: id, AssignmentID: task.AssignmentID}); err != nil {
			t.Fatal(err)
		}
	}
	if got := f.task(third.ID); got.State != "queued" {
		t.Fatalf("exceeded capacity: %+v", got)
	}
	cmd1 := f.bash(f.task(first.ID), "same-operation-key", "printf first")
	cmd2 := f.bash(f.task(second.ID), "same-operation-key", "printf second")
	if cmd1.ID == cmd2.ID {
		t.Fatal("concurrent task command identities collided")
	}
	f.finish(cmd1, "exited")
	if _, err := f.agent("task_complete", Args{TaskID: first.ID, AssignmentID: f.task(first.ID).AssignmentID, Outcome: "succeeded", Summary: "first done"}); err != nil {
		t.Fatal(err)
	}
	f.b.tick(f.ctx)
	if got := f.task(third.ID); got.State != "offered" {
		t.Fatalf("freed capacity was not scheduled: %+v", got)
	}
}

func TestConcurrentRecoveryReservesSlotsBeforeNewTasks(t *testing.T) {
	f := newFixture(t)
	configure := func() {
		t.Helper()
		if _, err := f.b.CallAgent(f.ctx, f.claude, f.connectionID, "agent_recovery_configure", model.JSON(map[string]any{"auto_resume": true, "max_concurrent_tasks": 2})); err != nil {
			t.Fatal(err)
		}
	}
	configure()
	one, two := f.running("resume-one"), f.running("resume-two")
	f.b.AgentDisconnected(f.claude, f.connectionID)
	var notices []model.ChannelNotification
	f.connectionID = "concurrent-reconnect"
	if err := f.b.AgentConnected(f.ctx, f.claude, f.connectionID, func(_ context.Context, n model.ChannelNotification) error { notices = append(notices, n); return nil }); err != nil {
		t.Fatal(err)
	}
	configure()
	third := f.submit("new-after-disconnect", "new task must wait for recovery capacity")
	f.b.tick(f.ctx)
	if len(notices) != 2 {
		t.Fatalf("lost concurrent resume notices: %+v", notices)
	}
	if f.task(third.ID).State != "queued" {
		t.Fatal("new task stole a saved conversation's slot")
	}
	for _, old := range []model.Task{one, two} {
		current := f.task(old.ID)
		resumed := f.claimResume(old.ID, current.ResumeRequest.ID)
		if resumed.AssignmentID == old.AssignmentID || resumed.LeaseEpoch != old.LeaseEpoch+1 {
			t.Fatal("recovery reused old execution authority")
		}
	}
	f.b.tick(f.ctx)
	if f.task(third.ID).State != "queued" {
		t.Fatal("offered more work than the agent can run")
	}
}
