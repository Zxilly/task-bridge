package broker

import (
	"context"
	"testing"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
)

func enableRecovery(t *testing.T, f *brokerFixture) {
	t.Helper()
	_, err := f.b.CallAgent(f.ctx, f.claude, f.connectionID, "agent_recovery_configure", model.JSON(map[string]bool{"auto_resume": true}))
	if err != nil {
		t.Fatal(err)
	}
}

func TestAutomaticRecoveryWaitsForCommandAndFencesAssignment(t *testing.T) {
	f := newFixture(t)
	enableRecovery(t, f)
	task := f.running("network-reset")
	command := f.bash(task, "execute-once", "printf once")
	f.b.AgentDisconnected(f.claude, f.connectionID)
	interrupted := f.task(task.ID)
	if interrupted.ResumeRequest == nil || interrupted.ResumeRequest.State != "pending" {
		t.Fatalf("disconnect lost automatic recovery: %+v", interrupted)
	}
	var notices []model.ChannelNotification
	f.connectionID = "reconnected"
	if err := f.b.AgentConnected(f.ctx, f.claude, f.connectionID, func(_ context.Context, n model.ChannelNotification) error { notices = append(notices, n); return nil }); err != nil {
		t.Fatal(err)
	}
	f.b.tick(f.ctx)
	if len(notices) != 0 {
		t.Fatal("offered recovery before command result was known")
	}
	f.finish(command, "exited")
	f.b.tick(f.ctx)
	if len(notices) != 1 || notices[0].Meta.ResumeRequestID != interrupted.ResumeRequest.ID {
		t.Fatalf("missing recovery notification: %+v", notices)
	}
	resumed := f.claimResume(task.ID, interrupted.ResumeRequest.ID)
	if resumed.ID != task.ID || resumed.AssignmentID == task.AssignmentID || resumed.DeadlineAt != task.DeadlineAt {
		t.Fatalf("bad automatic assignment: %+v", resumed)
	}
	_, err := f.agent("bash", Args{TaskID: task.ID, AssignmentID: task.AssignmentID, OperationKey: "stale", Command: "false"})
	requireCode(t, err, "stale_assignment")
	if again := f.bash(resumed, "execute-once", "printf once"); again.ID != command.ID {
		t.Fatal("recovery replayed a completed command")
	}
}

func TestAutomaticRecoverySurvivesBrokerRestartButNotProcessFailure(t *testing.T) {
	f := newFixture(t)
	enableRecovery(t, f)
	task := f.running("broker-restart")
	restarted := New(f.store, 100)
	if err := restarted.Recover(f.ctx); err != nil {
		t.Fatal(err)
	}
	if got := f.task(task.ID); got.ResumeRequest == nil || got.ResumeRequest.State != "pending" {
		t.Fatalf("restart lost recovery: %+v", got)
	}
	// A Claude failure is a different interruption; disconnecting afterwards
	// must not convert it into an automatic retry loop.
	f2 := newFixture(t)
	enableRecovery(t, f2)
	failed := f2.running("claude-failure")
	if _, err := f2.agent("task_interrupt", Args{TaskID: failed.ID, AssignmentID: failed.AssignmentID, ErrorCode: "claude_rate_limited", Message: "Account limit reached."}); err != nil {
		t.Fatal(err)
	}
	f2.b.AgentDisconnected(f2.claude, f2.connectionID)
	if got := f2.task(failed.ID); got.ResumeRequest != nil {
		t.Fatal("automatically retried a Claude failure")
	}
	// An unknown command remains fenced even after the agent reconnects.
	if err := f.store.Update(f.ctx, func(tx *storage.Tx) error {
		w, err := tx.Workspace(task.WorkspaceID)
		if err != nil {
			return err
		}
		w.Quarantined = true
		return tx.SaveWorkspace(w)
	}); err != nil {
		t.Fatal(err)
	}
	f.b.AgentDisconnected(f.claude, f.connectionID)
	f.reconnectAgent("after-quarantine")
	_, err := f.agent("task_resume", Args{TaskID: task.ID, ResumeRequestID: f.task(task.ID).ResumeRequest.ID})
	requireCode(t, err, "reconciliation_required")
}

func TestAutomaticRecoveryDoesNotOfferUnknownCommandOrCancelledTask(t *testing.T) {
	for _, mode := range []string{"unknown", "cancelled", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			enableRecovery(t, f)
			task := f.running("blocked-recovery")
			if mode == "cancelled" {
				if _, err := f.b.CallClient(f.ctx, f.peer, "tasks_cancel", model.JSON(map[string]string{"task_id": task.ID, "reason": "stop"})); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "revoked" {
				if err := f.b.Revoke(f.ctx, f.executor.ID); err != nil {
					t.Fatal(err)
				}
			}
			var cmd model.Command
			if mode == "unknown" {
				cmd = f.bash(task, "uncertain", "printf once")
			}
			f.b.AgentDisconnected(f.claude, f.connectionID)
			if mode == "unknown" {
				f.finish(cmd, "unknown")
			}
			var notices []model.ChannelNotification
			f.connectionID = "blocked-new"
			if err := f.b.AgentConnected(f.ctx, f.claude, f.connectionID, func(_ context.Context, n model.ChannelNotification) error { notices = append(notices, n); return nil }); err != nil {
				t.Fatal(err)
			}
			f.b.tick(f.ctx)
			for _, notice := range notices {
				if notice.Meta.ResumeRequestID != "" {
					t.Fatal("unsafe automatic recovery was offered")
				}
			}
		})
	}
}
