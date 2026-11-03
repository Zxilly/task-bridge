package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
)

func TestWorkerRunsSameWorkspaceTasksAndCommandsConcurrently(t *testing.T) {
	f := setupWorkerMCPResume(t)
	f.worker.cfg.MaxConcurrentTasks = 2
	sessions := make(chan Session, 4)
	f.start(t, func(ctx context.Context, s Session) error {
		relay, err := taskClient(ctx, s)
		if err != nil {
			return err
		}
		defer relay.Close()
		// Both commands must enter Bash before the test releases either.
		command := fmt.Sprintf("printf started > started-%s; while [ ! -f release ]; do sleep 0.05; done; printf done", s.Task.TaskID)
		result, err := workerRelayTool[model.CommandResult](ctx, relay, "bash", map[string]any{"task_id": s.Task.TaskID, "assignment_id": s.Task.AssignmentID, "operation_key": "concurrent-command", "command": command, "wait_ms": 1})
		if err != nil {
			return err
		}
		sessions <- s
		result, err = workerRelayTool[model.CommandResult](ctx, relay, "command_wait", map[string]any{"task_id": s.Task.TaskID, "command_id": result.Command.ID, "wait_ms": 10000})
		if err != nil {
			return err
		}
		if result.Command.State != "exited" {
			return fmt.Errorf("unexpected command state %s", result.Command.State)
		}
		_, err = workerRelayTool[model.Task](ctx, relay, "task_complete", map[string]string{"task_id": s.Task.TaskID, "assignment_id": s.Task.AssignmentID, "outcome": "succeeded", "summary": "concurrent task completed"})
		return err
	})
	first := workerMCPTool[model.Task](t, f, "tasks_submit", model.TaskSubmitArgs{WorkspaceID: "workspace", Prompt: "First concurrent task.", IdempotencyKey: "concurrent-first", DeadlineMS: 45000})
	second := workerMCPTool[model.Task](t, f, "tasks_submit", model.TaskSubmitArgs{WorkspaceID: "workspace", Prompt: "Second concurrent task.", IdempotencyKey: "concurrent-second", DeadlineMS: 45000})
	a, b := f.nextSession(t, sessions), f.nextSession(t, sessions)
	if a.ID == b.ID || a.Directory == b.Directory || a.Token == b.Token || a.Task.AssignmentID == b.Task.AssignmentID {
		t.Fatal("concurrent tasks shared session state or authorization")
	}
	deadline := time.After(5 * time.Second)
	for {
		_, err1 := os.Stat(filepath.Join(f.workspace, "started-"+first.ID))
		_, err2 := os.Stat(filepath.Join(f.workspace, "started-"+second.ID))
		if err1 == nil && err2 == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("same-workspace B commands did not overlap")
		case <-time.After(20 * time.Millisecond):
		}
	}
	// Each task's relay must reject the neighboring task even on one connection.
	relay, err := taskClient(f.ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	_, err = workerRelayTool[model.TaskSnapshot](f.ctx, relay, "task_get", map[string]string{"task_id": b.Task.TaskID})
	_ = relay.Close()
	if err == nil {
		t.Fatal("task relay exposed another concurrent task")
	}
	if err := os.WriteFile(filepath.Join(f.workspace, "release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{first.ID, second.ID} {
		f.waitTask(t, id, "concurrent completion", func(task model.Task) bool { return task.State == "succeeded" })
	}
}

func TestConcurrentTasksRecoverTheirOwnConversationsAfterConnectionLoss(t *testing.T) {
	f := setupWorkerMCPResume(t)
	f.worker.cfg.MaxConcurrentTasks = 2
	f.worker.cfg.AutoResume = true
	sockets := proxyWorkerConnection(t, f)
	sessions := make(chan Session, 8)
	f.start(t, func(ctx context.Context, s Session) error {
		relay, err := taskClient(ctx, s)
		if err != nil {
			return err
		}
		defer relay.Close()
		cmd, err := workerRelayTool[model.CommandResult](ctx, relay, "bash", map[string]any{"task_id": s.Task.TaskID, "assignment_id": s.Task.AssignmentID, "operation_key": "once", "command": fmt.Sprintf("printf 'once\\n' >> proof-%s", s.Task.TaskID), "wait_ms": 10000})
		if err != nil {
			return err
		}
		if cmd.Command.State != "exited" {
			return fmt.Errorf("command state %s", cmd.Command.State)
		}
		sessions <- s
		if !s.Resume {
			<-ctx.Done()
			return ctx.Err()
		}
		_, err = workerRelayTool[model.Task](ctx, relay, "task_complete", map[string]string{"task_id": s.Task.TaskID, "assignment_id": s.Task.AssignmentID, "outcome": "succeeded", "summary": "recovered original concurrent task"})
		return err
	})
	ids := []string{}
	for _, key := range []string{"recovery-one", "recovery-two"} {
		task := workerMCPTool[model.Task](t, f, "tasks_submit", model.TaskSubmitArgs{WorkspaceID: "workspace", Prompt: "Recover my own saved conversation.", IdempotencyKey: key, DeadlineMS: 45000})
		ids = append(ids, task.ID)
	}
	original := map[string]Session{}
	for range 2 {
		s := f.nextSession(t, sessions)
		original[s.Task.TaskID] = s
	}
	select {
	case socket := <-sockets:
		_ = socket.Close()
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	for range 2 {
		s := f.nextSession(t, sessions)
		old, ok := original[s.Task.TaskID]
		if !ok || !s.Resume || s.ID != old.ID || s.Directory != old.Directory || s.Task.AssignmentID == old.Task.AssignmentID || s.Token == old.Token {
			t.Fatalf("wrong concurrent recovery: %+v", s)
		}
	}
	for _, id := range ids {
		f.waitTask(t, id, "concurrent recovery completion", func(task model.Task) bool { return task.State == "succeeded" })
		data, err := os.ReadFile(filepath.Join(f.workspace, "proof-"+id))
		if err != nil || string(data) != "once\n" {
			t.Fatalf("replayed command for %s: %q (%v)", id, data, err)
		}
	}
}

func TestConcurrentTasksLaunchDistinctOSProcessesAndCancelIndependently(t *testing.T) {
	f := setupWorker(t)
	f.worker.cfg.MaxConcurrentTasks = 2
	launcher, _ := launcherHelper(t, "hold-task")
	sessions := make(chan Session, 4)
	f.worker.launcher = launcherFunc(func(ctx context.Context, s Session) error { sessions <- s; return launcher.Launch(ctx, s) })
	run := startFixtureController(t, f)
	first := f.submit(t, "process-one", "First independent Claude process.")
	second := f.submit(t, "process-two", "Second independent Claude process.")
	started := map[string]Session{}
	for range 2 {
		select {
		case s := <-sessions:
			started[s.Task.TaskID] = s
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		}
	}
	pids := map[string]int{}
	deadline := time.After(5 * time.Second)
	for len(pids) < 2 {
		for id, s := range started {
			if data, err := os.ReadFile(filepath.Join(s.Directory, "process.pid")); err == nil && len(data) > 0 {
				pid, err := strconv.Atoi(string(data))
				if err != nil {
					t.Fatal(err)
				}
				pids[id] = pid
			}
		}
		select {
		case <-deadline:
			t.Fatal("both independent processes did not start")
		case <-time.After(20 * time.Millisecond):
		}
	}
	if pids[first.ID] == pids[second.ID] {
		t.Fatal("two tasks used the same OS process")
	}
	if _, err := f.broker.CallClient(f.ctx, f.peer, "tasks_cancel", model.JSON(map[string]string{"task_id": first.ID, "reason": "cancel one concurrent task"})); err != nil {
		t.Fatal(err)
	}
	waitWorkerTask(t, f, first.ID, "cancelled")
	// The other process must remain alive long enough to observe its release.
	neighbor := started[second.ID]
	if err := os.WriteFile(filepath.Join(neighbor.Directory, "release-process"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	interrupted := waitWorkerTask(t, f, second.ID, "interrupted")
	if interrupted.Error == nil || interrupted.Error.Code != "claude_session_incomplete" {
		t.Fatalf("neighbor did not exit normally after its own release: %+v", interrupted.Error)
	}
	if _, err := os.Stat(filepath.Join(neighbor.Directory, "released-process")); err != nil {
		t.Fatal("cancelling one task killed its neighboring process")
	}
	run.requireAlive(t)
}

func TestWorkerTaskFailureDoesNotStopConcurrentNeighbor(t *testing.T) {
	f := setupWorkerMCPResume(t)
	f.worker.cfg.MaxConcurrentTasks = 2
	f.worker.cfg.AutoResume = true
	sessions := make(chan Session, 4)
	fail := make(chan struct{})
	release := make(chan struct{})
	f.start(t, func(ctx context.Context, s Session) error {
		sessions <- s
		if s.Model == "opus" {
			select {
			case <-fail:
				return errors.New("one task failed")
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		relay, err := taskClient(ctx, s)
		if err != nil {
			return err
		}
		defer relay.Close()
		_, err = workerRelayTool[model.Task](ctx, relay, "task_complete", map[string]string{"task_id": s.Task.TaskID, "assignment_id": s.Task.AssignmentID, "outcome": "succeeded", "summary": "neighbor survived"})
		return err
	})
	failed := workerMCPTool[model.Task](t, f, "tasks_submit", model.TaskSubmitArgs{WorkspaceID: "workspace", Prompt: "Fail only this task.", IdempotencyKey: "failure-isolation", Model: "opus", DeadlineMS: 45000})
	neighbor := workerMCPTool[model.Task](t, f, "tasks_submit", model.TaskSubmitArgs{WorkspaceID: "workspace", Prompt: "Keep the neighboring task running.", IdempotencyKey: "neighbor", Model: "sonnet", DeadlineMS: 45000})
	f.nextSession(t, sessions)
	f.nextSession(t, sessions)
	close(fail)
	interrupted := f.waitTask(t, failed.ID, "isolated failure", func(task model.Task) bool { return task.State == "interrupted" })
	stillRunning := workerMCPTool[model.TaskSnapshot](t, f, "tasks_get", model.TaskGetArgs{TaskID: neighbor.ID}).Task
	if stillRunning.State != "running" || stillRunning.ConnectionID != interrupted.ConnectionID {
		t.Fatal("one child failure stopped or disconnected its neighbor")
	}
	close(release)
	f.waitTask(t, neighbor.ID, "neighbor completion", func(task model.Task) bool { return task.State == "succeeded" })
}
