package agent

import (
	"context"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
)

func waitWorkerTask(t *testing.T, f *workerFixture, id, state string) model.Task {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		var task model.Task
		if err := f.broker.Store.View(f.ctx, func(tx *storage.Tx) (err error) { task, err = tx.Task(id); return err }); err != nil {
			t.Fatal(err)
		}
		if task.State == state {
			return task
		}
		select {
		case <-deadline.C:
			t.Fatalf("task state=%s, want %s", task.State, state)
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

type fixtureController struct {
	cancel   context.CancelFunc
	finished chan struct{}
	err      error
}

func startFixtureController(t *testing.T, f *workerFixture) *fixtureController {
	t.Helper()
	ctx, cancel := context.WithCancel(f.ctx)
	run := &fixtureController{cancel: cancel, finished: make(chan struct{})}
	go func() { run.err = f.worker.Run(ctx); close(run.finished) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-run.finished:
			if run.err != nil {
				t.Errorf("worker shutdown: %v", run.err)
			}
		case <-time.After(5 * time.Second):
			t.Error("worker did not stop after cancellation")
		}
	})
	return run
}

func (r *fixtureController) requireAlive(t *testing.T) {
	t.Helper()
	select {
	case <-r.finished:
		t.Fatalf("worker unexpectedly stopped: %v", r.err)
	default:
	}
}
