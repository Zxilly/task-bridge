package agent

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/Zxilly/task-bridge/internal/model"
)

// Cut a real TCP connection while the real B executor and journal stay alive.
// Recovery must preserve Claude context and execute an operation only once.
func TestWorkerAutomaticallyResumesAfterRepeatedConnectionLoss(t *testing.T) {
	f := setupWorkerMCPResume(t)
	u, err := url.Parse(f.worker.cfg.ServerURL)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	sockets := make(chan net.Conn, 8)
	upstreamAddress := u.Host
	go func() {
		for {
			in, err := listener.Accept()
			if err != nil {
				return
			}
			out, err := net.Dial("tcp", upstreamAddress)
			if err != nil {
				_ = in.Close()
				return
			}
			sockets <- in
			go func() { defer in.Close(); defer out.Close(); _, _ = io.Copy(out, in) }()
			go func() { defer in.Close(); defer out.Close(); _, _ = io.Copy(in, out) }()
		}
	}()
	u.Host = listener.Addr().String()
	f.worker.cfg.ServerURL = u.String()
	f.worker.cfg.AutoResume = true
	sessions := make(chan Session, 4)
	var launches atomic.Int32
	f.start(t, func(ctx context.Context, s Session) error {
		attempt := launches.Add(1)
		relay, err := taskClient(ctx, s)
		if err != nil {
			return err
		}
		defer relay.Close()
		cmd, err := workerRelayTool[model.CommandResult](ctx, relay, "bash", map[string]any{"task_id": s.Task.TaskID, "assignment_id": s.Task.AssignmentID, "operation_key": "once-across-network-resets", "command": "printf 'once\\n' >> automatic-proof.txt", "wait_ms": 10000})
		if err != nil {
			return err
		}
		if cmd.Command.State != "exited" {
			return fmt.Errorf("command state %s", cmd.Command.State)
		}
		sessions <- s
		if attempt < 3 {
			<-ctx.Done()
			return ctx.Err()
		}
		_, err = workerRelayTool[model.Task](ctx, relay, "task_complete", map[string]string{"task_id": s.Task.TaskID, "assignment_id": s.Task.AssignmentID, "outcome": "succeeded", "summary": "Automatically recovered twice"})
		return err
	})
	task := workerMCPTool[model.Task](t, f, "tasks_submit", model.TaskSubmitArgs{WorkspaceID: "workspace", Prompt: "Recover the same conversation after a network reset.", IdempotencyKey: "automatic-resume", DeadlineMS: 45000})
	first := f.nextSession(t, sessions)
	previous := first
	for range 2 {
		select {
		case socket := <-sockets:
			_ = socket.Close()
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		}
		next := f.nextSession(t, sessions)
		if !next.Resume || next.ID != first.ID || next.Directory != first.Directory || next.Token == previous.Token || next.Task.AssignmentID == previous.Task.AssignmentID {
			t.Fatalf("lost context or reused authority: %+v", next)
		}
		previous = next
	}
	finished := f.waitTask(t, task.ID, "automatic recovery completion", func(t model.Task) bool { return t.State == "succeeded" })
	if finished.DeadlineAt != task.DeadlineAt || finished.LeaseEpoch != 3 || finished.ResumeRequest == nil || !finished.ResumeRequest.Automatic {
		t.Fatalf("bad recovery receipt: %+v", finished)
	}
	data, err := os.ReadFile(filepath.Join(f.workspace, "automatic-proof.txt"))
	if err != nil || string(data) != "once\n" {
		t.Fatalf("command was replayed: %q, %v", data, err)
	}
}
