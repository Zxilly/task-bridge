package broker

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
	"github.com/google/uuid"
)

type brokerFixture struct {
	t                *testing.T
	ctx              context.Context
	b                *Broker
	store            *storage.Store
	executor, claude model.Principal
	peer             *ClientPeer
	connectionID     string
}

func newFixture(t *testing.T) *brokerFixture {
	t.Helper()
	ctx := context.Background()
	store, err := storage.Open(ctx, filepath.Join(t.TempDir(), "broker.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	b := New(store, 100)
	if err = b.CreateWorkspace(ctx, model.Workspace{ID: "workspace-1", ExecutorID: "executor-1"}); err != nil {
		t.Fatal(err)
	}
	executor, _, err := b.CreateDevice(ctx, model.Principal{Kind: "executor", ExecutorID: "executor-1"})
	if err != nil {
		t.Fatal(err)
	}
	claude, _, err := b.CreateDevice(ctx, model.Principal{Kind: "claude", AgentID: "claude-1", WorkspaceID: "workspace-1"})
	if err != nil {
		t.Fatal(err)
	}
	peer, err := b.ConnectClient(ctx, executor, "journal-1", func(model.Envelope) error { return nil }, func() {})
	if err != nil {
		t.Fatal(err)
	}
	f := &brokerFixture{t: t, ctx: ctx, b: b, store: store, executor: executor, claude: claude, peer: peer, connectionID: "connection-1"}
	if err = b.AgentConnected(ctx, claude, f.connectionID, func(context.Context, model.ChannelNotification) error { return nil }); err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *brokerFixture) submit(key, prompt string) model.Task {
	f.t.Helper()
	value, err := f.b.CallClient(f.ctx, f.peer, "tasks_submit", model.JSON(Args{WorkspaceID: "workspace-1", IdempotencyKey: key, Prompt: prompt}))
	if err != nil {
		f.t.Fatal(err)
	}
	return value.(model.Task)
}
func (f *brokerFixture) task(id string) model.Task {
	f.t.Helper()
	var task model.Task
	if err := f.store.View(f.ctx, func(tx *storage.Tx) error { var err error; task, err = tx.Task(id); return err }); err != nil {
		f.t.Fatal(err)
	}
	return task
}
func (f *brokerFixture) command(id string) model.Command {
	f.t.Helper()
	var command model.Command
	if err := f.store.View(f.ctx, func(tx *storage.Tx) error { var err error; command, err = tx.Command(id); return err }); err != nil {
		f.t.Fatal(err)
	}
	return command
}
func (f *brokerFixture) workspace() model.Workspace {
	f.t.Helper()
	var workspace model.Workspace
	if err := f.store.View(f.ctx, func(tx *storage.Tx) error { var err error; workspace, err = tx.Workspace("workspace-1"); return err }); err != nil {
		f.t.Fatal(err)
	}
	return workspace
}
func (f *brokerFixture) agent(method string, args any) (any, error) {
	return f.b.CallAgent(f.ctx, f.claude, f.connectionID, method, model.JSON(args))
}
func (f *brokerFixture) running(key string) model.Task {
	f.t.Helper()
	task := f.submit(key, "test task")
	f.b.tick(f.ctx)
	task = f.task(task.ID)
	if task.State != "offered" {
		f.t.Fatalf("task state=%s", task.State)
	}
	if _, err := f.agent("task_accept", Args{TaskID: task.ID, AssignmentID: task.AssignmentID}); err != nil {
		f.t.Fatal(err)
	}
	return f.task(task.ID)
}
func (f *brokerFixture) bash(task model.Task, key, command string) model.Command {
	f.t.Helper()
	value, err := f.agent("bash", Args{TaskID: task.ID, AssignmentID: task.AssignmentID, OperationKey: key, Command: command, WaitMS: 1})
	if err != nil {
		f.t.Fatal(err)
	}
	return value.(model.CommandResult).Command
}
func (f *brokerFixture) event(typ string, event model.CommandEvent) model.Envelope {
	return model.Envelope{Version: 1, Type: typ, ID: uuid.NewString(), ServerEpoch: f.store.Epoch, Generation: f.peer.Generation, StreamID: f.peer.Stream, Payload: model.JSON(event)}
}
func (f *brokerFixture) finish(command model.Command, state string) {
	f.t.Helper()
	code := 0
	if err := f.b.ClientEvent(f.ctx, f.peer, f.event("command.finished", model.CommandEvent{CommandID: command.ID, State: state, ExitCode: &code, FinishedAt: model.Now()})); err != nil {
		f.t.Fatal(err)
	}
}
func requireCode(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || model.AsError(err).Code != want {
		t.Fatalf("error=%v, want code=%s", err, want)
	}
}

func TestTaskAndCommandIdempotency(t *testing.T) {
	f := newFixture(t)
	first := f.submit("task-key", "same prompt")
	again := f.submit("task-key", "same prompt")
	if again.ID != first.ID {
		t.Fatal("submission retry created a new task")
	}
	_, err := f.b.CallClient(f.ctx, f.peer, "tasks_submit", model.JSON(Args{WorkspaceID: "workspace-1", IdempotencyKey: "task-key", Prompt: "different prompt"}))
	requireCode(t, err, "idempotency_conflict")
	f.b.tick(f.ctx)
	task := f.task(first.ID)
	if _, err = f.agent("task_accept", Args{TaskID: task.ID, AssignmentID: task.AssignmentID}); err != nil {
		t.Fatal(err)
	}
	command := f.bash(task, "operation-key", "printf once")
	againCommand := f.bash(task, "operation-key", "printf once")
	if againCommand.ID != command.ID {
		t.Fatal("command retry created another command")
	}
	_, err = f.agent("bash", Args{TaskID: task.ID, AssignmentID: task.AssignmentID, OperationKey: "operation-key", Command: "printf twice", WaitMS: 1})
	requireCode(t, err, "idempotency_conflict")
	if err = f.store.View(f.ctx, func(tx *storage.Tx) error {
		commands, err := tx.Commands(task.ID)
		if len(commands) != 1 {
			t.Fatalf("stored commands=%d", len(commands))
		}
		messages, err2 := tx.Outbox(f.executor.ExecutorID)
		if len(messages) != 1 {
			t.Fatalf("dispatch messages=%d", len(messages))
		}
		if err != nil {
			return err
		}
		return err2
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDisconnectRequiresExplicitResumeAndFencesOldAssignment(t *testing.T) {
	f := newFixture(t)
	task := f.running("resume")
	f.b.AgentDisconnected(f.claude, f.connectionID)
	interrupted := f.task(task.ID)
	if interrupted.State != "interrupted" || interrupted.LeaseUntil != 0 {
		t.Fatalf("bad interruption: %+v", interrupted)
	}
	f.connectionID = "connection-2"
	if err := f.b.AgentConnected(f.ctx, f.claude, f.connectionID, func(context.Context, model.ChannelNotification) error { return nil }); err != nil {
		t.Fatal(err)
	}
	_, err := f.agent("task_accept", Args{TaskID: task.ID, AssignmentID: task.AssignmentID})
	requireCode(t, err, "stale_assignment")
	_, err = f.agent("bash", Args{TaskID: task.ID, AssignmentID: task.AssignmentID, OperationKey: "before-resume", Command: "true", WaitMS: 1})
	requireCode(t, err, "stale_assignment")
	resumed, err := f.agent("task_resume", Args{TaskID: task.ID, PreviousAssignmentID: task.AssignmentID})
	if err != nil {
		t.Fatal(err)
	}
	newTask := resumed.(model.Task)
	if newTask.AssignmentID == task.AssignmentID || newTask.LeaseEpoch != task.LeaseEpoch+1 || newTask.ConnectionID != f.connectionID {
		t.Fatalf("resume failed to fence old assignment: %+v", newTask)
	}
	_, err = f.agent("bash", Args{TaskID: task.ID, AssignmentID: task.AssignmentID, OperationKey: "after-resume-old", Command: "true", WaitMS: 1})
	requireCode(t, err, "stale_assignment")
	f.bash(newTask, "after-resume-new", "true")
	if err = f.b.CreateWorkspace(f.ctx, model.Workspace{ID: "workspace-2", ExecutorID: "executor-2"}); err != nil {
		t.Fatal(err)
	}
	other, _, err := f.b.CreateDevice(f.ctx, model.Principal{Kind: "claude", AgentID: "claude-2", WorkspaceID: "workspace-2"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.b.CallAgent(f.ctx, other, "other-connection", "task_get", model.JSON(Args{TaskID: task.ID}))
	requireCode(t, err, "forbidden")
}

func TestUnknownRequiresAuditedResolutionAndWorkspaceRelease(t *testing.T) {
	f := newFixture(t)
	task := f.running("unknown")
	command := f.bash(task, "op", "true")
	f.finish(command, "unknown")
	if f.task(task.ID).State != "interrupted" || !f.workspace().Quarantined {
		t.Fatal("unknown result failed to interrupt and quarantine")
	}
	_, err := f.agent("task_resume", Args{TaskID: task.ID, PreviousAssignmentID: task.AssignmentID})
	requireCode(t, err, "reconciliation_required")
	_, err = f.b.Admin(f.ctx, "workspaces.release", model.JSON(model.AdminArgs{ID: "workspace-1", Reason: "checked"}))
	requireCode(t, err, "reconciliation_required")
	if _, err = f.b.Admin(f.ctx, "commands.resolve", model.JSON(model.AdminArgs{ID: command.ID, Outcome: "stopped", Reason: "verified process and descendants stopped"})); err != nil {
		t.Fatal(err)
	}
	if f.command(command.ID).State != "cancelled" || !f.workspace().Quarantined {
		t.Fatal("resolution must not implicitly release workspace")
	}
	if _, err = f.b.Admin(f.ctx, "workspaces.release", model.JSON(model.AdminArgs{ID: "workspace-1", Reason: "verified no live processes"})); err != nil {
		t.Fatal(err)
	}
	resumed, err := f.agent("task_resume", Args{TaskID: task.ID, PreviousAssignmentID: task.AssignmentID})
	if err != nil {
		t.Fatal(err)
	}
	newTask := resumed.(model.Task)
	if _, err = f.agent("task_complete", Args{TaskID: task.ID, AssignmentID: newTask.AssignmentID, Outcome: "failed", Summary: "execution was stopped and reconciled"}); err != nil {
		t.Fatal(err)
	}
	if f.task(task.ID).State != "failed" {
		t.Fatal("task was not completed after reconciliation")
	}
}

func TestAbandonPreservesUnknownAndBlocksWorkspaceUntilResolved(t *testing.T) {
	f := newFixture(t)
	task := f.running("abandon")
	command := f.bash(task, "op", "true")
	f.finish(command, "unknown")
	if _, err := f.b.Admin(f.ctx, "tasks.abandon", model.JSON(model.AdminArgs{ID: task.ID, Reason: "owner abandoned uncertain task"})); err != nil {
		t.Fatal(err)
	}
	abandoned := f.task(task.ID)
	if abandoned.State != "abandoned" || !abandoned.Unresolved || f.command(command.ID).State != "unknown" || !f.workspace().Quarantined {
		t.Fatalf("abandon discarded uncertainty: %+v", abandoned)
	}
	_, err := f.b.CallClient(f.ctx, f.peer, "tasks_submit", model.JSON(Args{WorkspaceID: "workspace-1", IdempotencyKey: "blocked", Prompt: "next task"}))
	requireCode(t, err, "workspace_quarantined")
	if _, err = f.b.Admin(f.ctx, "commands.resolve", model.JSON(model.AdminArgs{ID: command.ID, Outcome: "not_started", Reason: "audited journal confirms no process was launched"})); err != nil {
		t.Fatal(err)
	}
	if _, err = f.b.Admin(f.ctx, "workspaces.release", model.JSON(model.AdminArgs{ID: "workspace-1", Reason: "verified no old process can execute"})); err != nil {
		t.Fatal(err)
	}
	if f.workspace().Quarantined {
		t.Fatal("workspace still quarantined")
	}
	f.submit("after-release", "next task")
}

func TestDeadlineCancelsCommandBeforeTaskBecomesTimedOut(t *testing.T) {
	f := newFixture(t)
	task := f.running("deadline")
	command := f.bash(task, "op", "true")
	if err := f.store.Update(f.ctx, func(tx *storage.Tx) error {
		task, err := tx.Task(task.ID)
		if err != nil {
			return err
		}
		task.DeadlineAt = model.Now() - 1
		return tx.SaveTask(task)
	}); err != nil {
		t.Fatal(err)
	}
	f.b.tick(f.ctx)
	if got := f.task(task.ID); got.State != "cancel_requested" || got.LeaseUntil != 0 {
		t.Fatalf("deadline pretended command had stopped: %+v", got)
	}
	if got := f.command(command.ID); got.CancelReason != "deadline" {
		t.Fatalf("command cancellation reason=%q", got.CancelReason)
	}
	f.finish(command, "cancelled")
	if got := f.task(task.ID); got.State != "timed_out" {
		t.Fatalf("confirmed cancellation did not finalize deadline: %+v", got)
	}
}

func TestDuplicateTaskCompletionRejectsConflictingResult(t *testing.T) {
	f := newFixture(t)
	task := f.running("complete")
	args := Args{TaskID: task.ID, AssignmentID: task.AssignmentID, Outcome: "succeeded", Summary: "done", Result: json.RawMessage(`{"value":1}`)}
	if _, err := f.agent("task_complete", args); err != nil {
		t.Fatal(err)
	}
	if _, err := f.agent("task_complete", args); err != nil {
		t.Fatalf("identical final retry failed: %v", err)
	}
	args.Result = json.RawMessage(`{"value":2}`)
	_, err := f.agent("task_complete", args)
	requireCode(t, err, "result_conflict")
	var final map[string]int
	if err = json.Unmarshal(f.task(task.ID).Result, &final); err != nil {
		t.Fatal(err)
	}
	if final["value"] != 1 {
		t.Fatal("final result was overwritten")
	}
}

func TestTaskCompletionCanonicalizesJSONWithoutLosingIntegerPrecision(t *testing.T) {
	f := newFixture(t)
	task := f.running("canonical-completion")
	complete := func(result string) error {
		raw := fmt.Sprintf(`{"task_id":%q,"assignment_id":%q,"outcome":"succeeded","summary":"done","result":%s}`, task.ID, task.AssignmentID, result)
		_, err := f.b.CallAgent(f.ctx, f.claude, f.connectionID, "task_complete", json.RawMessage(raw))
		return err
	}
	if err := complete(`{ "z": [3, {"b": false, "a": 9007199254740993}], "a": {"y": "ok", "x": null} }`); err != nil {
		t.Fatal(err)
	}
	if err := complete(`{"a":{"x":null,"y":"ok"},"z":[3,{"a":9007199254740993,"b":false}]}`); err != nil {
		t.Fatalf("equivalent reordered JSON was rejected: %v", err)
	}
	requireCode(t, complete(`{"a":{"x":null,"y":"ok"},"z":[3,{"a":9007199254740992,"b":false}]}`), "result_conflict")
	want := `{"a":{"x":null,"y":"ok"},"z":[3,{"a":9007199254740993,"b":false}]}`
	if got := string(f.task(task.ID).Result); got != want {
		t.Fatalf("stored canonical result=%s, want %s", got, want)
	}
}

func TestExecutorJournalBindingSurvivesBrokerRecreation(t *testing.T) {
	f := newFixture(t)
	initialGeneration := f.peer.Generation
	f.b.DisconnectClient(f.peer)
	f.b = New(f.store, 100)
	connect := func(stream string) (*ClientPeer, error) {
		return f.b.ConnectClient(f.ctx, f.executor, stream, func(model.Envelope) error { return nil }, func() {})
	}
	_, err := connect("replacement-journal")
	requireCode(t, err, "journal_mismatch")
	peer, err := connect("journal-1")
	if err != nil {
		t.Fatalf("original journal could not reconnect: %v", err)
	}
	if peer.Generation != initialGeneration+1 {
		t.Fatalf("rejected journal changed generation: initial=%d next=%d", initialGeneration, peer.Generation)
	}
	f.b.DisconnectClient(peer)
	if err = f.store.View(f.ctx, func(tx *storage.Tx) error {
		var stream string
		if err := tx.QueryRowContext(f.ctx, "SELECT stream FROM peer_cursors WHERE peer=?", f.executor.ExecutorID).Scan(&stream); err != nil {
			return err
		}
		if stream != "journal-1" {
			t.Fatalf("journal binding changed to %q", stream)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestExecutorJournalResetRequiresStoppedExecutionAndRotatesCredential(t *testing.T) {
	f := newFixture(t)
	reset := func(reason string) error {
		_, err := f.b.Admin(f.ctx, "executors.reset_journal", model.JSON(model.AdminArgs{ID: f.executor.ExecutorID, Reason: reason}))
		return err
	}
	requireCode(t, reset(""), "invalid_arguments")
	requireCode(t, reset("verified no live processes"), "executor_online")
	task := f.running("journal-reset")
	command := f.bash(task, "op", "true")
	f.b.DisconnectClient(f.peer)
	requireCode(t, reset("executor offline but execution outcome missing"), "reconciliation_required")
	var err error
	f.peer, err = f.b.ConnectClient(f.ctx, f.executor, "journal-1", func(model.Envelope) error { return nil }, func() {})
	if err != nil {
		t.Fatalf("rejected reset revoked the existing credential: %v", err)
	}
	f.finish(command, "unknown")
	f.b.DisconnectClient(f.peer)
	requireCode(t, reset("unknown outcome is unresolved"), "reconciliation_required")
	if _, err = f.b.Admin(f.ctx, "commands.resolve", model.JSON(model.AdminArgs{ID: command.ID, Outcome: "stopped", Reason: "verified process and descendants stopped"})); err != nil {
		t.Fatal(err)
	}
	const reason = "all executor processes and descendants verified stopped; journal replaced"
	if err = reset(reason); err != nil {
		t.Fatal(err)
	}
	if err = f.store.View(f.ctx, func(tx *storage.Tx) error {
		principal, err := tx.Principal(f.executor.ID)
		if err != nil {
			return err
		}
		if !principal.Disabled {
			t.Fatal("reset retained an active old credential")
		}
		var count int
		if err = tx.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM peer_cursors WHERE peer=?", f.executor.ExecutorID).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			t.Fatal("reset retained the old journal binding")
		}
		if err = tx.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM resolutions WHERE target=? AND outcome='journal_binding_reset' AND reason=? AND actor='local-admin'", f.executor.ExecutorID, reason).Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			t.Fatalf("successful reset audit records=%d", count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	_, err = f.b.ConnectClient(f.ctx, f.executor, "replacement-journal", func(model.Envelope) error { return nil }, func() {})
	requireCode(t, err, "unauthorized")
	rotated, token, err := f.b.CreateDevice(f.ctx, model.Principal{Kind: "executor", ExecutorID: f.executor.ExecutorID})
	if err != nil {
		t.Fatal(err)
	}
	authenticated, err := f.b.Authenticate(f.ctx, token)
	if err != nil || authenticated.ID != rotated.ID {
		t.Fatalf("rotated credential authentication failed: principal=%+v err=%v", authenticated, err)
	}
	peer, err := f.b.ConnectClient(f.ctx, rotated, "replacement-journal", func(model.Envelope) error { return nil }, func() {})
	if err != nil {
		t.Fatal(err)
	}
	if peer.Generation <= f.peer.Generation || peer.Stream != "replacement-journal" {
		t.Fatalf("reset did not preserve generation fencing: %+v", peer)
	}
	if !f.workspace().Quarantined {
		t.Fatal("journal reset implicitly released workspace quarantine")
	}
}

func TestExactOutboxAckAndGenerationFencing(t *testing.T) {
	f := newFixture(t)
	task := f.running("ack")
	command := f.bash(task, "op", "true")
	if _, err := f.agent("command_cancel", Args{TaskID: task.ID, CommandID: command.ID}); err != nil {
		t.Fatal(err)
	}
	var messages []model.Envelope
	if err := f.store.View(f.ctx, func(tx *storage.Tx) error {
		var err error
		messages, err = tx.Outbox(f.executor.ExecutorID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 {
		t.Fatalf("outbox count=%d", len(messages))
	}
	var cancelID, dispatchID string
	for _, message := range messages {
		if message.Type == "command.cancel" {
			cancelID = message.ID
		} else {
			dispatchID = message.ID
		}
	}
	if err := f.b.Ack(f.ctx, f.peer, cancelID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.View(f.ctx, func(tx *storage.Tx) error {
		pending, err := tx.Outbox(f.executor.ExecutorID)
		if len(pending) != 1 || pending[0].ID != dispatchID {
			t.Fatalf("ACK incorrectly removed another message: %+v", pending)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	oldPeer := f.peer
	f.b.DisconnectClient(oldPeer)
	var err error
	f.peer, err = f.b.ConnectClient(f.ctx, f.executor, "journal-1", func(model.Envelope) error { return nil }, func() {})
	if err != nil {
		t.Fatal(err)
	}
	if f.peer.Generation <= oldPeer.Generation {
		t.Fatal("generation did not advance")
	}
	requireCode(t, f.b.Ack(f.ctx, oldPeer, dispatchID), "stale_connection")
	message := f.event("command.finished", model.CommandEvent{CommandID: command.ID, State: "cancelled"})
	message.Generation = oldPeer.Generation
	requireCode(t, f.b.ClientEvent(f.ctx, f.peer, message), "stale_connection")
	message.Generation = f.peer.Generation
	if err = f.b.ClientEvent(f.ctx, f.peer, message); err != nil {
		t.Fatal(err)
	}
	if err = f.b.ClientEvent(f.ctx, f.peer, message); err != nil {
		t.Fatalf("identical event retry failed: %v", err)
	}
	message.Payload = model.JSON(model.CommandEvent{CommandID: command.ID, State: "unknown"})
	requireCode(t, f.b.ClientEvent(f.ctx, f.peer, message), "message_conflict")
}

func TestBrokerRecoveryRequiresExplicitResume(t *testing.T) {
	f := newFixture(t)
	task := f.running("recover")
	f.b.DisconnectClient(f.peer)
	if err := f.b.Recover(f.ctx); err != nil {
		t.Fatal(err)
	}
	if got := f.task(task.ID); got.State != "interrupted" || got.LeaseUntil != 0 {
		t.Fatalf("recover did not revoke lease: %+v", got)
	}
	_, err := f.agent("task_resume", Args{TaskID: task.ID, PreviousAssignmentID: task.AssignmentID})
	requireCode(t, err, "executor_unavailable")
	f.peer, err = f.b.ConnectClient(f.ctx, f.executor, "journal-1", func(model.Envelope) error { return nil }, func() {})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.agent("task_resume", Args{TaskID: task.ID, PreviousAssignmentID: task.AssignmentID}); err != nil {
		t.Fatal(err)
	}
}

func TestCommandTerminalResultRejectsDifferentExitCode(t *testing.T) {
	f := newFixture(t)
	task := f.running("command-final")
	command := f.bash(task, "op", "true")
	zero := 0
	event := model.CommandEvent{CommandID: command.ID, State: "exited", ExitCode: &zero, FinishedAt: model.Now(), DurationMS: 10}
	if err := f.b.ClientEvent(f.ctx, f.peer, f.event("command.finished", event)); err != nil {
		t.Fatal(err)
	}
	// Distinct delivery IDs do not permit contradicting the persisted fact.
	one := 1
	event.ExitCode = &one
	requireCode(t, f.b.ClientEvent(f.ctx, f.peer, f.event("command.finished", event)), "terminal_conflict")
	if got := f.command(command.ID); got.ExitCode == nil || *got.ExitCode != 0 {
		t.Fatalf("final exit code was replaced: %+v", got)
	}
}

func TestOutputBytePagesPreserveSplitUTF8AndReadBeyond4096Chunks(t *testing.T) {
	f := newFixture(t)
	task := f.running("output")
	command := f.bash(task, "op", "true")
	original := []byte("仓")
	if err := f.store.Update(f.ctx, func(tx *storage.Tx) error {
		for i, value := range original {
			if _, err := tx.AddChunk(model.Chunk{CommandID: command.ID, Seq: int64(i + 1), Stream: "stdout", Data: []byte{value}}); err != nil {
				return err
			}
		}
		for i := len(original); i < 4100; i++ {
			if _, err := tx.AddChunk(model.Chunk{CommandID: command.ID, Seq: int64(i + 1), Stream: "stdout", Data: []byte("x")}); err != nil {
				return err
			}
		}
		command.OutputBytes = 4100
		return tx.SaveCommand(command)
	}); err != nil {
		t.Fatal(err)
	}
	type page struct {
		Cursor  int64  `json:"cursor"`
		HasMore bool   `json:"has_more"`
		Stdout  string `json:"stdout"`
		Chunks  []struct {
			Data []byte `json:"data"`
		} `json:"chunks"`
	}
	read := func(cursor int64, limit int) page {
		t.Helper()
		var result model.OutputResult
		if err := f.store.View(f.ctx, func(tx *storage.Tx) error {
			var err error
			result, err = output(tx, command, cursor, limit)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		var p page
		if err := json.Unmarshal(model.JSON(result), &p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	var reconstructed []byte
	for cursor := int64(0); cursor < int64(len(original)); {
		p := read(cursor, 1)
		if p.Cursor <= cursor {
			t.Fatal("output cursor did not advance")
		}
		for _, chunk := range p.Chunks {
			reconstructed = append(reconstructed, chunk.Data...)
		}
		if p.Stdout != "" {
			t.Fatalf("partial UTF-8 page presented corrupt display text %q", p.Stdout)
		}
		cursor = p.Cursor
	}
	if !bytes.Equal(reconstructed, original) {
		t.Fatalf("UTF-8 bytes lost: %x, want %x", reconstructed, original)
	}
	p := read(4096, 4)
	if p.Cursor != 4100 || p.HasMore {
		t.Fatalf("late chunks stranded output cursor: %+v", p)
	}
}

func TestRevokedHandshakeAndOldCredentialCannotCloseRotatedAgent(t *testing.T) {
	f := newFixture(t)
	f.b.DisconnectClient(f.peer)
	if err := f.b.Revoke(f.ctx, f.executor.ID); err != nil {
		t.Fatal(err)
	}
	_, err := f.b.ConnectClient(f.ctx, f.executor, "journal-1", func(model.Envelope) error { return nil }, func() {})
	requireCode(t, err, "unauthorized")
	f.b.AgentDisconnected(f.claude, f.connectionID)
	if err = f.b.Revoke(f.ctx, f.claude.ID); err != nil {
		t.Fatal(err)
	}
	err = f.b.AgentConnected(f.ctx, f.claude, "stale-handshake", func(context.Context, model.ChannelNotification) error { return nil })
	requireCode(t, err, "unauthorized")
	rotated, _, err := f.b.CreateDevice(f.ctx, model.Principal{Kind: "claude", AgentID: f.claude.AgentID, WorkspaceID: f.claude.WorkspaceID})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	f.b.RegisterAgentCloser("rotated", func() { closed = true })
	if err = f.b.AgentConnected(f.ctx, rotated, "rotated", func(context.Context, model.ChannelNotification) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err = f.b.Revoke(f.ctx, f.claude.ID); err != nil {
		t.Fatal(err)
	}
	if closed {
		t.Fatal("revoking old credentials closed rotated agent")
	}
}

func TestAbandonMissingExecutorPreservesUncertaintyAndCanBeReconciled(t *testing.T) {
	f := newFixture(t)
	task := f.running("missing-executor")
	command := f.bash(task, "op", "true")
	f.b.DisconnectClient(f.peer)
	if _, err := f.b.Admin(f.ctx, "tasks.abandon", model.JSON(model.AdminArgs{ID: task.ID, Reason: "executor was permanently lost"})); err != nil {
		t.Fatal(err)
	}
	if got := f.command(command.ID); got.State != "unknown" {
		t.Fatalf("abandon left an unreconcilable command or claimed it stopped: %+v", got)
	}
	if !f.workspace().Quarantined {
		t.Fatal("abandon released workspace before manual verification")
	}
	if err := f.store.View(f.ctx, func(tx *storage.Tx) error {
		messages, err := tx.Outbox(f.executor.ExecutorID)
		for _, message := range messages {
			if message.Type == "command.cancel" {
				return err
			}
		}
		t.Fatal("abandon did not retain cancellation request")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.b.Admin(f.ctx, "commands.resolve", model.JSON(model.AdminArgs{ID: command.ID, Outcome: "stopped", Reason: "operator verified machine powered off and disk retired"})); err != nil {
		t.Fatal(err)
	}
	if _, err := f.b.Admin(f.ctx, "workspaces.release", model.JSON(model.AdminArgs{ID: "workspace-1", Reason: "no previous execution can resume"})); err != nil {
		t.Fatal(err)
	}
	if f.workspace().Quarantined {
		t.Fatal("verified workspace could not be released")
	}
}

func TestDuplicateChunkSequenceRejectsContradictoryBytes(t *testing.T) {
	f := newFixture(t)
	task := f.running("chunk-duplicate")
	command := f.bash(task, "op", "true")
	event := model.CommandEvent{CommandID: command.ID, Chunk: &model.Chunk{CommandID: command.ID, Seq: 1, Stream: "stdout", Data: []byte("first")}}
	if err := f.b.ClientEvent(f.ctx, f.peer, f.event("command.output", event)); err != nil {
		t.Fatal(err)
	}
	if err := f.b.ClientEvent(f.ctx, f.peer, f.event("command.output", event)); err != nil {
		t.Fatalf("identical chunk with a new envelope should deduplicate: %v", err)
	}
	event.Chunk.Data = []byte("second")
	if err := f.b.ClientEvent(f.ctx, f.peer, f.event("command.output", event)); err == nil {
		t.Fatal("contradictory bytes for the same chunk sequence were acknowledged")
	}
	if got := f.command(command.ID); got.OutputBytes != 5 {
		t.Fatalf("duplicate chunk changed output accounting: %+v", got)
	}
}

func TestStorageAdmissionPreservesRetriesAndTerminalEvidence(t *testing.T) {
	f := newFixture(t)
	task := f.running("capacity")
	command := f.bash(task, "existing-op", "true")
	// Force only the admission policy to reject new work; SQLite itself
	// remains writable so existing work can persist its final evidence.
	f.store.MaxBytes = 1
	if got := f.submit("capacity", "test task"); got.ID != task.ID {
		t.Fatal("capacity caused task retry to create a new record")
	}
	if got := f.bash(task, "existing-op", "true"); got.ID != command.ID {
		t.Fatal("capacity caused command retry to create a new record")
	}
	_, err := f.b.CallClient(f.ctx, f.peer, "tasks_submit", model.JSON(Args{WorkspaceID: "workspace-1", Prompt: "new work", IdempotencyKey: "rejected-new"}))
	requireCode(t, err, "storage_full")
	_, err = f.agent("bash", Args{TaskID: task.ID, AssignmentID: task.AssignmentID, OperationKey: "new-op", Command: "true", WaitMS: 1})
	requireCode(t, err, "storage_full")
	chunk := model.CommandEvent{CommandID: command.ID, Chunk: &model.Chunk{CommandID: command.ID, Seq: 1, Stream: "stdout", Data: []byte("dropped output")}}
	if err = f.b.ClientEvent(f.ctx, f.peer, f.event("command.output", chunk)); err != nil {
		t.Fatalf("capacity blocked output truncation accounting: %v", err)
	}
	if err = f.b.ClientEvent(f.ctx, f.peer, f.event("command.output", chunk)); err != nil {
		t.Fatalf("duplicate truncated output failed: %v", err)
	}
	got := f.command(command.ID)
	if got.OutputBytes != 0 || got.DroppedBytes != int64(len(chunk.Chunk.Data)) || !got.Truncated {
		t.Fatalf("capacity output accounting wrong or counted duplicate: %+v", got)
	}
	f.finish(command, "exited")
	got = f.command(command.ID)
	if got.State != "exited" || got.ExitCode == nil || *got.ExitCode != 0 {
		t.Fatalf("capacity lost terminal result: %+v", got)
	}
	if _, err = f.agent("task_complete", Args{TaskID: task.ID, AssignmentID: task.AssignmentID, Outcome: "succeeded", Summary: "command completed with output truncated"}); err != nil {
		t.Fatalf("capacity blocked task completion: %v", err)
	}
}

func journalRecord(task model.Task, command model.Command, state string) reconciliationRecord {
	return reconciliationRecord{CommandID: command.ID, TaskID: task.ID, WorkspaceID: task.WorkspaceID, State: state, Command: command.Command, OperationKey: command.OperationKey, Cwd: command.RequestedCwd, TimeoutMS: command.TimeoutMS, ExitCode: command.ExitCode, Signal: command.Signal, StartedAt: command.StartedAt, FinishedAt: command.FinishedAt, DurationMS: command.DurationMS}
}

func TestExecutorReconciliationMatchesEvidenceAndQuarantinesOrphans(t *testing.T) {
	f := newFixture(t)
	task := f.running("reconcile")
	command := f.bash(task, "op", "true")
	f.finish(command, "unknown")
	zero := 0
	record := journalRecord(task, command, "exited")
	record.ExitCode = &zero
	record.FinishedAt = model.Now()
	record.DurationMS = 10
	orphan := reconciliationRecord{CommandID: "orphan-command", TaskID: "task-absent-from-snapshot", WorkspaceID: task.WorkspaceID, State: "unknown", Command: "printf orphan", OperationKey: "orphan-op", TimeoutMS: 120000}
	value, err := f.b.CallClient(f.ctx, f.peer, "executor_reconcile", model.JSON(model.ExecutorReconcileArgs{Reason: "operator approved journal comparison", Records: []reconciliationRecord{record, orphan}}))
	if err != nil {
		t.Fatal(err)
	}
	var response struct {
		Known   []string `json:"known_command_ids"`
		Orphans []string `json:"orphan_command_ids"`
	}
	if err = json.Unmarshal(model.JSON(value), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Known) != 1 || response.Known[0] != command.ID || len(response.Orphans) != 1 || response.Orphans[0] != orphan.CommandID {
		t.Fatalf("wrong reconciliation classification: %+v", response)
	}
	if got := f.command(command.ID); got.State != "exited" || got.ExitCode == nil || *got.ExitCode != 0 {
		t.Fatalf("durable journal result was not restored: %+v", got)
	}
	if got := f.task(task.ID); got.State != "interrupted" || got.LeaseUntil != 0 {
		t.Fatalf("reconciliation implicitly resumed execution: %+v", got)
	}
	if !f.workspace().Quarantined {
		t.Fatal("reconciliation implicitly cleared quarantine")
	}
	if err = f.store.View(f.ctx, func(tx *storage.Tx) error {
		_, err := tx.Command(orphan.CommandID)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("orphan was materialized as executable command: %v", err)
		}
		var count int
		if err = tx.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM resolutions WHERE target IN (?,?)", command.ID, orphan.CommandID).Scan(&count); err != nil {
			return err
		}
		if count != 2 {
			t.Fatalf("reconciliation audit records=%d", count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestExecutorReconciliationRejectsFingerprintScopeAndKnownResultConflict(t *testing.T) {
	f := newFixture(t)
	task := f.running("reconcile-conflict")
	command := f.bash(task, "op", "true")
	zero := 0
	finished := model.Now()
	event := model.CommandEvent{CommandID: command.ID, State: "exited", ExitCode: &zero, FinishedAt: finished, DurationMS: 10}
	if err := f.b.ClientEvent(f.ctx, f.peer, f.event("command.finished", event)); err != nil {
		t.Fatal(err)
	}
	command = f.command(command.ID)
	call := func(record reconciliationRecord) error {
		_, err := f.b.CallClient(f.ctx, f.peer, "executor_reconcile", model.JSON(model.ExecutorReconcileArgs{Reason: "verify conflicting journal evidence", Records: []reconciliationRecord{record}}))
		return err
	}
	record := journalRecord(task, command, "exited")
	record.Command = "different command"
	requireCode(t, call(record), "reconciliation_conflict")
	record = journalRecord(task, command, "exited")
	one := 1
	record.ExitCode = &one
	requireCode(t, call(record), "terminal_conflict")
	if err := f.b.CreateWorkspace(f.ctx, model.Workspace{ID: "other-workspace", ExecutorID: "other-executor"}); err != nil {
		t.Fatal(err)
	}
	record = journalRecord(task, command, "exited")
	record.WorkspaceID = "other-workspace"
	requireCode(t, call(record), "forbidden")
	if got := f.command(command.ID); got.ExitCode == nil || *got.ExitCode != 0 || got.Command != "true" {
		t.Fatalf("rejected evidence mutated known result: %+v", got)
	}
}

func TestExecutorReconciliationReturnsKnownServerResultForLocalUnknown(t *testing.T) {
	f := newFixture(t)
	task := f.running("known-server-result")
	command := f.bash(task, "op", "true")
	zero := 0
	finished := model.Now()
	event := model.CommandEvent{CommandID: command.ID, State: "exited", ExitCode: &zero, FinishedAt: finished, DurationMS: 10}
	if err := f.b.ClientEvent(f.ctx, f.peer, f.event("command.finished", event)); err != nil {
		t.Fatal(err)
	}
	record := journalRecord(task, command, "unknown")
	value, err := f.b.CallClient(f.ctx, f.peer, "executor_reconcile", model.JSON(model.ExecutorReconcileArgs{Reason: "local process was lost but broker retained the final result", Records: []reconciliationRecord{record}}))
	if err != nil {
		t.Fatal(err)
	}
	var response struct {
		Known   []string                      `json:"known_command_ids"`
		Results map[string]model.CommandEvent `json:"server_results"`
	}
	if err = json.Unmarshal(model.JSON(value), &response); err != nil {
		t.Fatal(err)
	}
	result, ok := response.Results[command.ID]
	if !ok || result.CommandID != command.ID || result.State != "exited" || result.ExitCode == nil || *result.ExitCode != 0 || result.FinishedAt != finished {
		t.Fatalf("known evidence was not returned: %+v", response)
	}
	if got := f.command(command.ID); got.State != "exited" || got.ExitCode == nil || *got.ExitCode != 0 {
		t.Fatalf("local unknown erased known server evidence: %+v", got)
	}
	if !f.workspace().Quarantined {
		t.Fatal("result adoption implicitly released workspace")
	}
}

func TestSchedulingUsesExecutorReportedWorkspaceSubset(t *testing.T) {
	f := newFixture(t)
	if err := f.b.CreateWorkspace(f.ctx, model.Workspace{ID: "workspace-2", ExecutorID: f.executor.ExecutorID}); err != nil {
		t.Fatal(err)
	}
	secondAgent, _, err := f.b.CreateDevice(f.ctx, model.Principal{Kind: "claude", AgentID: "claude-2", WorkspaceID: "workspace-2"})
	if err != nil {
		t.Fatal(err)
	}
	notices := map[string]int{}
	offers := map[string]model.ChannelNotification{}
	notify := func(_ context.Context, message model.ChannelNotification) error {
		notices[message.Meta.WorkspaceID]++
		offers[message.Meta.WorkspaceID] = message
		return nil
	}
	checkOffer := func(task model.Task) {
		t.Helper()
		notification := offers[task.WorkspaceID]
		want := model.ChannelMetadata{TaskID: task.ID, AssignmentID: task.AssignmentID, WorkspaceID: task.WorkspaceID}
		if notification.Meta != want {
			t.Fatalf("channel routing metadata=%+v want=%+v", notification.Meta, want)
		}
		if !strings.HasPrefix(notification.Content, "New task.") || !strings.HasSuffix(notification.Content, task.Prompt) {
			t.Fatalf("channel content lost task instructions or prompt: %q", notification.Content)
		}
	}
	f.b.AgentDisconnected(f.claude, f.connectionID)
	if err = f.b.AgentConnected(f.ctx, f.claude, f.connectionID, notify); err != nil {
		t.Fatal(err)
	}
	if err = f.b.AgentConnected(f.ctx, secondAgent, "connection-2", notify); err != nil {
		t.Fatal(err)
	}
	f.b.DisconnectClient(f.peer)
	f.peer, err = f.b.ConnectClient(f.ctx, f.executor, "journal-1", func(model.Envelope) error { return nil }, func() {}, "workspace-2")
	if err != nil {
		t.Fatal(err)
	}
	first := f.submit("workspace-1-task", "wait for this workspace to be configured")
	value, err := f.b.CallClient(f.ctx, f.peer, "tasks_submit", model.JSON(Args{WorkspaceID: "workspace-2", IdempotencyKey: "workspace-2-task", Prompt: "this workspace is configured"}))
	if err != nil {
		t.Fatal(err)
	}
	second := value.(model.Task)
	f.b.tick(f.ctx)
	if got := f.task(first.ID); got.State != "queued" || got.AssignmentID != "" || got.AgentID != "" {
		t.Fatalf("unreported workspace was assigned: %+v", got)
	}
	if got := f.task(second.ID); got.State != "offered" || got.AgentID != secondAgent.AgentID {
		t.Fatalf("reported workspace was not assigned: %+v", got)
	}
	if notices["workspace-1"] != 0 || notices["workspace-2"] != 1 {
		t.Fatalf("incorrect channel offers for reported subset: %+v", notices)
	}
	checkOffer(f.task(second.ID))
	f.b.DisconnectClient(f.peer)
	f.peer, err = f.b.ConnectClient(f.ctx, f.executor, "journal-1", func(model.Envelope) error { return nil }, func() {}, "workspace-1", "workspace-2")
	if err != nil {
		t.Fatal(err)
	}
	f.b.tick(f.ctx)
	if got := f.task(first.ID); got.State != "offered" || got.AgentID != f.claude.AgentID {
		t.Fatalf("newly reported workspace was not assigned: %+v", got)
	}
	if notices["workspace-1"] != 1 {
		t.Fatalf("newly reported workspace channel offers=%d", notices["workspace-1"])
	}
	checkOffer(f.task(first.ID))
}

func TestRunningTaskChecksCurrentExecutorWorkspaceConfiguration(t *testing.T) {
	f := newFixture(t)
	if err := f.b.CreateWorkspace(f.ctx, model.Workspace{ID: "workspace-2", ExecutorID: f.executor.ExecutorID}); err != nil {
		t.Fatal(err)
	}
	task := f.running("executor-workspace-change")
	command := f.bash(task, "first", "true")
	authorize := func() bool {
		t.Helper()
		value, err := f.b.CallClient(f.ctx, f.peer, "command_authorize", model.JSON(Args{CommandID: command.ID, AssignmentID: command.AssignmentID}))
		if err != nil {
			t.Fatal(err)
		}
		return value.(model.CommandAuthorization).Allowed
	}
	if !authorize() {
		t.Fatal("initial command was not authorized")
	}
	reconnect := func(workspace string) {
		t.Helper()
		f.b.DisconnectClient(f.peer)
		var err error
		f.peer, err = f.b.ConnectClient(f.ctx, f.executor, "journal-1", func(model.Envelope) error { return nil }, func() {}, workspace)
		if err != nil {
			t.Fatal(err)
		}
	}
	reconnect("workspace-2")
	if authorize() {
		t.Fatal("command authorized after its workspace was removed from executor configuration")
	}
	_, err := f.agent("bash", Args{TaskID: task.ID, AssignmentID: task.AssignmentID, OperationKey: "second", Command: "true", WaitMS: 1})
	requireCode(t, err, "executor_unavailable")
	if err = f.store.View(f.ctx, func(tx *storage.Tx) error {
		commands, err := tx.Commands(task.ID)
		if len(commands) != 1 || commands[0].ID != command.ID {
			t.Fatalf("rejected execution created a command: %+v", commands)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	reconnect("workspace-1")
	if !authorize() {
		t.Fatal("command remained unauthorized after its workspace was configured again")
	}
	f.finish(command, "exited")
	f.bash(task, "second", "true")
}

func TestNotStartedExecutionDiagnosticsAreBoundedAndReturned(t *testing.T) {
	for _, test := range []struct {
		name   string
		detail model.Error
		reject bool
	}{
		{name: "shell_unavailable", detail: model.Error{Code: "shell_unavailable", Message: "configured shell executable does not exist", Retryable: true}},
		{name: "exact_limits", detail: model.Error{Code: strings.Repeat("c", 64), Message: strings.Repeat("m", 2048)}},
		{name: "code_too_long", detail: model.Error{Code: strings.Repeat("c", 65), Message: "rejected diagnostic"}, reject: true},
		{name: "message_too_long", detail: model.Error{Code: "shell_unavailable", Message: strings.Repeat("m", 2049)}, reject: true},
		{name: "message_limit_is_bytes", detail: model.Error{Code: "shell_unavailable", Message: strings.Repeat("错", 683)}, reject: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t)
			task := f.running("diagnostic")
			command := f.bash(task, "op", "true")
			event := model.CommandEvent{CommandID: command.ID, State: "not_started", FinishedAt: model.Now(), Error: &test.detail}
			err := f.b.ClientEvent(f.ctx, f.peer, f.event("command.finished", event))
			if test.reject {
				requireCode(t, err, "invalid_message")
				if got := f.command(command.ID); got.State != "dispatched" || got.Error != nil {
					t.Fatalf("rejected diagnostic changed command state: %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := f.command(command.ID); got.State != "not_started" || got.Error == nil || *got.Error != test.detail || got.ExitCode != nil {
				t.Fatalf("execution diagnostic was not persisted: %+v", got)
			}
			value, err := f.agent("command_get", Args{TaskID: task.ID, CommandID: command.ID})
			if err != nil {
				t.Fatalf("command_get treated execution diagnostic as an RPC failure: %v", err)
			}
			result := value.(model.CommandResult)
			got := result.Command
			if result.Pending || got.State != "not_started" || got.Error == nil || *got.Error != test.detail {
				t.Fatalf("command_get lost execution diagnostic: %+v", result)
			}
		})
	}
}

func TestNonzeroCommandExitIsAnExecutionResult(t *testing.T) {
	f := newFixture(t)
	task := f.running("nonzero-exit")
	command := f.bash(task, "op", "exit 17")
	code := 17
	event := model.CommandEvent{CommandID: command.ID, State: "exited", ExitCode: &code, FinishedAt: model.Now()}
	if err := f.b.ClientEvent(f.ctx, f.peer, f.event("command.finished", event)); err != nil {
		t.Fatalf("nonzero process exit was treated as a protocol failure: %v", err)
	}
	value, err := f.agent("command_get", Args{TaskID: task.ID, CommandID: command.ID})
	if err != nil {
		t.Fatalf("nonzero process exit made command_get fail: %v", err)
	}
	result := value.(model.CommandResult)
	got := result.Command
	if result.Pending || got.State != "exited" || got.ExitCode == nil || *got.ExitCode != code || got.Error != nil {
		t.Fatalf("nonzero process exit was not returned intact: %+v", result)
	}
	if got := f.task(task.ID); got.State != "running" || f.workspace().Quarantined {
		t.Fatalf("nonzero process exit interrupted task or quarantined workspace: %+v", got)
	}
}
