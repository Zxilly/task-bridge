package broker

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
)

func (f *brokerFixture) requestResume(taskID, key string) model.TaskResumeRequest {
	f.t.Helper()
	value, err := f.b.CallClient(f.ctx, f.peer, "tasks_resume", model.JSON(model.TaskResumeArgs{TaskID: taskID, IdempotencyKey: key}))
	if err != nil {
		f.t.Fatal(err)
	}
	result, ok := value.(model.TaskResumeResult)
	if !ok || result.TaskID != taskID || result.ResumeRequest.TaskID != taskID {
		f.t.Fatalf("unexpected resume result: %#v", value)
	}
	return result.ResumeRequest
}

func (f *brokerFixture) reconnectAgent(id string) {
	f.t.Helper()
	f.connectionID = id
	if err := f.b.AgentConnected(f.ctx, f.claude, id, func(context.Context, model.ChannelNotification) error { return nil }); err != nil {
		f.t.Fatal(err)
	}
}

func (f *brokerFixture) claimResume(taskID, requestID string) model.Task {
	f.t.Helper()
	value, err := f.agent("task_resume", Args{TaskID: taskID, ResumeRequestID: requestID})
	if err != nil {
		f.t.Fatal(err)
	}
	return value.(model.Task)
}

func TestTaskResumeReceiptSurvivesAttemptsAndTerminalTask(t *testing.T) {
	f := newFixture(t)
	original := f.running("resume-history")
	f.b.AgentDisconnected(f.claude, f.connectionID)
	first := f.requestResume(original.ID, "resume-first")
	if first.State != "pending" || first.AgentID != original.AgentID || first.PreviousAssignmentID != original.AssignmentID {
		t.Fatalf("bad offline receipt: %+v", first)
	}
	if got := f.requestResume(original.ID, first.IdempotencyKey); got.ID != first.ID {
		t.Fatal("retry created a second request")
	}
	_, err := f.b.CallClient(f.ctx, f.peer, "tasks_resume", model.JSON(model.TaskResumeArgs{TaskID: original.ID, IdempotencyKey: "while-pending"}))
	requireCode(t, err, "resume_pending")
	f.reconnectAgent("resume-connection-2")
	_, err = f.agent("task_resume", Args{TaskID: original.ID, PreviousAssignmentID: original.AssignmentID})
	requireCode(t, err, "resume_request_required")
	claimed := f.claimResume(original.ID, first.ID)
	if claimed.State != "running" || claimed.AssignmentID == original.AssignmentID || claimed.LeaseEpoch != original.LeaseEpoch+1 || claimed.ResumeRequest.State != "accepted" || claimed.DeadlineAt != original.DeadlineAt {
		t.Fatalf("claim changed task identity/deadline or failed fencing: %+v", claimed)
	}
	_, err = f.agent("task_resume", Args{TaskID: original.ID, ResumeRequestID: first.ID})
	requireCode(t, err, "resume_already_claimed")
	f.b.AgentDisconnected(f.claude, f.connectionID)
	failed := f.requestResume(original.ID, first.IdempotencyKey)
	if failed.ID != first.ID || failed.State != "failed" || failed.Error == nil || failed.Error.Code != "agent_disconnected" {
		t.Fatalf("lost accepted attempt must remain consumed: %+v", failed)
	}
	f.reconnectAgent("resume-connection-3")
	second := f.requestResume(original.ID, "resume-second")
	if second.ID == first.ID || second.State != "pending" {
		t.Fatal("new key did not request another attempt")
	}
	if got := f.requestResume(original.ID, first.IdempotencyKey); got.ID != first.ID || got.State != "failed" {
		t.Fatalf("old key was overwritten: %+v", got)
	}
	if got := f.task(original.ID); got.ResumeRequest.ID != second.ID {
		t.Fatal("retrying old key replaced current request")
	}
	last := f.claimResume(original.ID, second.ID)
	if _, err = f.agent("task_complete", Args{TaskID: original.ID, AssignmentID: last.AssignmentID, Outcome: "succeeded", Summary: "finished original task"}); err != nil {
		t.Fatal(err)
	}
	for _, request := range []model.TaskResumeRequest{first, second} {
		if got := f.requestResume(original.ID, request.IdempotencyKey); got.ID != request.ID {
			t.Fatal("terminal task broke historical idempotency")
		}
	}
	_, err = f.b.CallClient(f.ctx, f.peer, "tasks_resume", model.JSON(model.TaskResumeArgs{TaskID: original.ID, IdempotencyKey: "after-terminal"}))
	requireCode(t, err, "invalid_state")
	var tasks, requests int
	if err := f.store.View(f.ctx, func(tx *storage.Tx) error {
		if err := tx.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM tasks").Scan(&tasks); err != nil {
			return err
		}
		return tx.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM task_resume_requests").Scan(&requests)
	}); err != nil {
		t.Fatal(err)
	}
	if tasks != 1 || requests != 2 {
		t.Fatalf("resume created extra work: tasks=%d requests=%d", tasks, requests)
	}
	other := f.submit("another-task", "another task")
	_, err = f.b.CallClient(f.ctx, f.peer, "tasks_resume", model.JSON(model.TaskResumeArgs{TaskID: other.ID, IdempotencyKey: first.IdempotencyKey}))
	requireCode(t, err, "idempotency_conflict")
}

func TestTaskResumeClaimIsConsumedOnceConcurrently(t *testing.T) {
	f := newFixture(t)
	task := f.running("resume-concurrent")
	f.b.AgentDisconnected(f.claude, f.connectionID)
	request := f.requestResume(task.ID, "one-attempt")
	f.reconnectAgent("claim-connection")
	results := make(chan error, 12)
	var workers sync.WaitGroup
	for range 12 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, err := f.agent("task_resume", Args{TaskID: task.ID, ResumeRequestID: request.ID})
			results <- err
		}()
	}
	workers.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else {
			requireCode(t, err, "resume_already_claimed")
		}
	}
	if successes != 1 || f.task(task.ID).LeaseEpoch != task.LeaseEpoch+1 {
		t.Fatalf("claim successes=%d", successes)
	}
}

func TestTaskResumeRequiresOriginalRequesterAndAgent(t *testing.T) {
	f := newFixture(t)
	task := f.running("resume-owner")
	f.b.AgentDisconnected(f.claude, f.connectionID)
	request := f.requestResume(task.ID, "owner-attempt")
	f.reconnectAgent("owner-new-connection")
	otherAgent, _, err := f.b.CreateDevice(f.ctx, model.Principal{Kind: "claude", AgentID: "other-agent", WorkspaceID: task.WorkspaceID})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.b.AgentConnected(f.ctx, otherAgent, "other-agent-connection", func(context.Context, model.ChannelNotification) error { return nil }); err != nil {
		t.Fatal(err)
	}
	_, err = f.b.CallAgent(f.ctx, otherAgent, "other-agent-connection", "task_resume", model.JSON(Args{TaskID: task.ID, ResumeRequestID: request.ID}))
	requireCode(t, err, "forbidden")
	_, err = f.b.CallAgent(f.ctx, f.claude, "old-connection", "task_resume", model.JSON(Args{TaskID: task.ID, ResumeRequestID: request.ID}))
	requireCode(t, err, "stale_assignment")
	_, err = f.agent("task_resume", Args{TaskID: task.ID, ResumeRequestID: request.ID, PreviousAssignmentID: task.AssignmentID})
	requireCode(t, err, "invalid_arguments")
	_, err = f.agent("task_resume", Args{TaskID: task.ID})
	requireCode(t, err, "invalid_arguments")
	if err = f.b.Revoke(f.ctx, f.executor.ID); err != nil {
		t.Fatal(err)
	}
	otherRequester, _, err := f.b.CreateDevice(f.ctx, model.Principal{Kind: "executor", ExecutorID: f.executor.ExecutorID})
	if err != nil {
		t.Fatal(err)
	}
	f.b.DisconnectClient(f.peer)
	f.peer, err = f.b.ConnectClient(f.ctx, otherRequester, "journal-1", func(model.Envelope) error { return nil }, func() {})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.b.CallClient(f.ctx, f.peer, "tasks_resume", model.JSON(model.TaskResumeArgs{TaskID: task.ID, IdempotencyKey: request.IdempotencyKey}))
	requireCode(t, err, "forbidden")
	if got := f.task(task.ID); got.ResumeRequest.State != "pending" || got.AssignmentID != task.AssignmentID {
		t.Fatal("rejected authorization changed task")
	}
}

func TestTaskResumeAdmissionRejectsUnsafeOrUnassignedTasks(t *testing.T) {
	cases := []struct {
		name, code string
		prepare    func(*brokerFixture, model.Task)
	}{
		{"running command", "reconciliation_required", func(f *brokerFixture, task model.Task) {
			f.bash(task, "op", "sleep 10")
			f.b.AgentDisconnected(f.claude, f.connectionID)
		}},
		{"unknown command", "reconciliation_required", func(f *brokerFixture, task model.Task) {
			command := f.bash(task, "op", "sleep 10")
			f.finish(command, "unknown")
		}},
		{"quarantined workspace", "reconciliation_required", func(f *brokerFixture, task model.Task) {
			f.b.AgentDisconnected(f.claude, f.connectionID)
			if err := f.store.Update(f.ctx, func(tx *storage.Tx) error {
				w, err := tx.Workspace(task.WorkspaceID)
				if err != nil {
					return err
				}
				w.Quarantined = true
				return tx.SaveWorkspace(w)
			}); err != nil {
				f.t.Fatal(err)
			}
		}},
		{"deadline", "deadline_exceeded", func(f *brokerFixture, task model.Task) {
			f.b.AgentDisconnected(f.claude, f.connectionID)
			if err := f.store.Update(f.ctx, func(tx *storage.Tx) error {
				current, err := tx.Task(task.ID)
				if err != nil {
					return err
				}
				current.DeadlineAt = model.Now() - 1
				return tx.SaveTask(current)
			}); err != nil {
				f.t.Fatal(err)
			}
		}},
		{"running", "invalid_state", func(*brokerFixture, model.Task) {}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			task := f.running("unsafe-resume")
			tc.prepare(f, task)
			_, err := f.b.CallClient(f.ctx, f.peer, "tasks_resume", model.JSON(model.TaskResumeArgs{TaskID: task.ID, IdempotencyKey: "unsafe"}))
			requireCode(t, err, tc.code)
			if f.task(task.ID).ResumeRequest != nil {
				t.Fatal("rejected request became durable")
			}
		})
	}
	for _, offered := range []bool{false, true} {
		f := newFixture(t)
		task := f.submit("new-task", "new task")
		if offered {
			f.b.tick(f.ctx)
		}
		_, err := f.b.CallClient(f.ctx, f.peer, "tasks_resume", model.JSON(model.TaskResumeArgs{TaskID: task.ID, IdempotencyKey: "not-interrupted"}))
		requireCode(t, err, "invalid_state")
	}
}

func TestTaskResumeClaimRechecksExecutionSafety(t *testing.T) {
	cases := []struct {
		name, code string
		change     func(*brokerFixture, model.Task)
	}{
		{"executor offline", "executor_unavailable", func(f *brokerFixture, _ model.Task) { f.b.DisconnectClient(f.peer) }},
		{"unknown command", "reconciliation_required", func(f *brokerFixture, task model.Task) {
			if err := f.store.Update(f.ctx, func(tx *storage.Tx) error {
				return tx.SaveCommand(model.Command{ID: "late-command", TaskID: task.ID, AssignmentID: task.AssignmentID, OperationKey: "late", State: "unknown"})
			}); err != nil {
				f.t.Fatal(err)
			}
		}},
		{"running command", "reconciliation_required", func(f *brokerFixture, task model.Task) {
			if err := f.store.Update(f.ctx, func(tx *storage.Tx) error {
				return tx.SaveCommand(model.Command{ID: "late-command", TaskID: task.ID, AssignmentID: task.AssignmentID, OperationKey: "late", State: "running"})
			}); err != nil {
				f.t.Fatal(err)
			}
		}},
		{"quarantine", "reconciliation_required", func(f *brokerFixture, task model.Task) {
			if err := f.store.Update(f.ctx, func(tx *storage.Tx) error {
				w, err := tx.Workspace(task.WorkspaceID)
				if err != nil {
					return err
				}
				w.Quarantined = true
				return tx.SaveWorkspace(w)
			}); err != nil {
				f.t.Fatal(err)
			}
		}},
		{"deadline", "deadline_exceeded", func(f *brokerFixture, task model.Task) {
			if err := f.store.Update(f.ctx, func(tx *storage.Tx) error {
				current, err := tx.Task(task.ID)
				if err != nil {
					return err
				}
				current.DeadlineAt = model.Now() - 1
				return tx.SaveTask(current)
			}); err != nil {
				f.t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			task := f.running("claim-recheck")
			f.b.AgentDisconnected(f.claude, f.connectionID)
			request := f.requestResume(task.ID, "pending-check")
			f.reconnectAgent("claim-new-connection")
			tc.change(f, task)
			_, err := f.agent("task_resume", Args{TaskID: task.ID, ResumeRequestID: request.ID})
			requireCode(t, err, tc.code)
			got := f.task(task.ID)
			if got.ResumeRequest.State != "pending" || got.AssignmentID != task.AssignmentID || got.LeaseUntil != 0 {
				t.Fatalf("unsafe claim mutated task: %+v", got)
			}
		})
	}
}

func TestTaskResumeFailureIsDurableAndFenced(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		t.Run(map[bool]string{false: "before claim", true: "after claim"}[claimed], func(t *testing.T) {
			f := newFixture(t)
			task := f.running("local-failure")
			f.b.AgentDisconnected(f.claude, f.connectionID)
			request := f.requestResume(task.ID, "missing-session")
			f.reconnectAgent("failure-controller")
			if claimed {
				f.claimResume(task.ID, request.ID)
			}
			args := Args{TaskID: task.ID, ResumeRequestID: request.ID, ErrorCode: "saved_session_missing", Message: "The original saved Claude session is unavailable."}
			_, err := f.b.CallAgent(f.ctx, f.claude, "other-connection", "task_resume_failed", model.JSON(args))
			requireCode(t, err, "stale_assignment")
			value, err := f.agent("task_resume_failed", args)
			if err != nil {
				t.Fatal(err)
			}
			receipt := value.(model.TaskResumeResult).ResumeRequest
			if receipt.State != "failed" || receipt.Error == nil || receipt.Error.Code != args.ErrorCode {
				t.Fatalf("failure lost: %+v", receipt)
			}
			got := f.task(task.ID)
			if got.State != "interrupted" || got.LeaseUntil != 0 || got.ResumeRequest.Error.Message != args.Message {
				t.Fatalf("failure did not fence: %+v", got)
			}
			args.Message = "A second report must not overwrite the first."
			value, err = f.agent("task_resume_failed", args)
			if err != nil || value.(model.TaskResumeResult).ResumeRequest.Error.Message != receipt.Error.Message {
				t.Fatal("failure replay changed original receipt")
			}
			if retry := f.requestResume(task.ID, request.IdempotencyKey); retry.State != "failed" {
				t.Fatal("B cannot inspect local failure")
			}
			_, err = f.agent("task_resume", Args{TaskID: task.ID, ResumeRequestID: request.ID})
			requireCode(t, err, "resume_failed")
			if next := f.requestResume(task.ID, "after-local-failure"); next.ID == request.ID {
				t.Fatal("new explicit attempt reused a consumed request")
			}
		})
	}
}

func TestTaskResumeFailureArgumentsAreBounded(t *testing.T) {
	f := newFixture(t)
	task := f.running("failure-limits")
	f.b.AgentDisconnected(f.claude, f.connectionID)
	request := f.requestResume(task.ID, "failure-limits-request")
	f.reconnectAgent("failure-limits-controller")
	for _, args := range []Args{
		{ErrorCode: "failure", Message: strings.Repeat("a", 1025)},
		{ErrorCode: "failure", Message: strings.Repeat("界", 342)},
		{ErrorCode: strings.Repeat("a", 65), Message: "failure"},
		{ErrorCode: "Failure", Message: "failure"},
		{ErrorCode: "failure", Message: "  "},
	} {
		args.TaskID, args.ResumeRequestID = task.ID, request.ID
		_, err := f.agent("task_resume_failed", args)
		requireCode(t, err, "invalid_arguments")
	}
	if got := f.task(task.ID); got.ResumeRequest.State != "pending" {
		t.Fatal("invalid failure report consumed request")
	}
	_, err := f.agent("task_resume_failed", Args{TaskID: task.ID, ResumeRequestID: request.ID, ErrorCode: "failure", Message: strings.Repeat("a", 1024)})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTaskResumePendingEndsOnCancellationDeadlineAndAbandon(t *testing.T) {
	for _, action := range []string{"cancel", "deadline", "abandon"} {
		t.Run(action, func(t *testing.T) {
			f := newFixture(t)
			task := f.running("pending-terminal")
			f.b.AgentDisconnected(f.claude, f.connectionID)
			request := f.requestResume(task.ID, "pending-attempt")
			var err error
			switch action {
			case "cancel":
				_, err = f.b.CallClient(f.ctx, f.peer, "tasks_cancel", model.JSON(model.TaskCancelArgs{TaskID: task.ID}))
			case "abandon":
				_, err = f.b.Admin(f.ctx, "tasks.abandon", model.JSON(model.AdminArgs{ID: task.ID, Reason: "operator ended task"}))
			case "deadline":
				err = f.store.Update(f.ctx, func(tx *storage.Tx) error {
					current, err := tx.Task(task.ID)
					if err != nil {
						return err
					}
					current.DeadlineAt = model.Now() - 1
					return tx.SaveTask(current)
				})
				f.b.tick(f.ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
			got := f.task(task.ID)
			if !got.Terminal() || got.ResumeRequest.State != "failed" {
				t.Fatalf("pending request survived terminal state: %+v", got)
			}
			if retry := f.requestResume(task.ID, request.IdempotencyKey); retry.ID != request.ID || retry.State != "failed" {
				t.Fatal("terminal retry lost receipt")
			}
		})
	}
}

func TestTaskResumePendingSurvivesRecoveryAndClaimedDoesNotReplay(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "accepted"}[claimed], func(t *testing.T) {
			f := newFixture(t)
			task := f.running("recovery")
			f.b.AgentDisconnected(f.claude, f.connectionID)
			request := f.requestResume(task.ID, "recovery-attempt")
			if claimed {
				f.reconnectAgent("recovery-controller")
				f.claimResume(task.ID, request.ID)
			}
			f.b = New(f.store, 100)
			if err := f.b.Recover(f.ctx); err != nil {
				t.Fatal(err)
			}
			var err error
			f.peer, err = f.b.ConnectClient(f.ctx, f.executor, "journal-1", func(model.Envelope) error { return nil }, func() {})
			if err != nil {
				t.Fatal(err)
			}
			f.reconnectAgent("after-broker-restart")
			retry := f.requestResume(task.ID, request.IdempotencyKey)
			if claimed {
				if retry.State != "failed" || retry.Error == nil || retry.Error.Code != "broker_restarted" {
					t.Fatalf("consumed request replayed after recovery: %+v", retry)
				}
				_, err = f.agent("task_resume", Args{TaskID: task.ID, ResumeRequestID: request.ID})
				requireCode(t, err, "resume_failed")
			} else {
				if retry.State != "pending" {
					t.Fatal("unconsumed request lost during recovery")
				}
				f.claimResume(task.ID, request.ID)
			}
		})
	}
}

func TestTaskResumeNotificationRetriesPendingAndStopsAfterClaim(t *testing.T) {
	f := newFixture(t)
	task := f.running("notifications")
	f.b.AgentDisconnected(f.claude, f.connectionID)
	notifications := []model.ChannelNotification{}
	f.connectionID = "notified-controller"
	if err := f.b.AgentConnected(f.ctx, f.claude, f.connectionID, func(_ context.Context, notification model.ChannelNotification) error {
		notifications = append(notifications, notification)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	f.b.tick(f.ctx) // Existing interruption notice must not delay explicit request.
	before := len(notifications)
	request := f.requestResume(task.ID, "notify-attempt")
	f.b.tick(f.ctx)
	if len(notifications) != before+1 || notifications[len(notifications)-1].Meta.ResumeRequestID != request.ID {
		t.Fatal("pending request did not notify immediately")
	}
	f.b.mu.Lock()
	f.b.agents[f.claude.AgentID].lastOffer = 0
	f.b.mu.Unlock()
	f.b.tick(f.ctx)
	if len(notifications) != before+2 || notifications[len(notifications)-1].Meta.ResumeRequestID != request.ID {
		t.Fatal("pending request was not repeated until claim")
	}
	f.claimResume(task.ID, request.ID)
	f.b.mu.Lock()
	f.b.agents[f.claude.AgentID].lastOffer = 0
	f.b.mu.Unlock()
	f.b.tick(f.ctx)
	if len(notifications) != before+2 {
		t.Fatal("consumed request repeated")
	}
}

func TestTaskResumeQueuedTaskRemainsBoundToOriginalAgent(t *testing.T) {
	f := newFixture(t)
	task := f.submit("queued-original", "original queued task")
	f.b.tick(f.ctx)
	task = f.task(task.ID)
	f.b.AgentDisconnected(f.claude, f.connectionID)
	other, _, err := f.b.CreateDevice(f.ctx, model.Principal{Kind: "claude", AgentID: "different-agent", WorkspaceID: task.WorkspaceID})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.b.AgentConnected(f.ctx, other, "different-connection", func(context.Context, model.ChannelNotification) error { return nil }); err != nil {
		t.Fatal(err)
	}
	f.b.tick(f.ctx)
	queued := f.task(task.ID)
	if queued.State != "queued" || queued.AgentID != task.AgentID {
		t.Fatalf("requeued task moved to another agent: %+v", queued)
	}
	request := f.requestResume(task.ID, "queued-attempt")
	if request.AgentID != task.AgentID || request.PreviousAssignmentID != "" || f.task(task.ID).State != "interrupted" {
		t.Fatalf("queued recovery changed owner: %+v", request)
	}
	f.reconnectAgent("queued-original-reconnected")
	f.claimResume(task.ID, request.ID)
}
