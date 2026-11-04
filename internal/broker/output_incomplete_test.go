package broker

import (
	"testing"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
)

func TestIncompleteCaptureDoesNotQuarantineOrBlockNextCommand(t *testing.T) {
	f := newFixture(t)
	task := f.running("inherited-pipe")
	command := f.bash(task, "background-shell", "true && sleep 3 & printf ready")
	code := 7
	if err := f.b.ClientEvent(f.ctx, f.peer, f.event("command.finished", model.CommandEvent{CommandID: command.ID, State: "exited", ExitCode: &code, OutputIncomplete: true, FinishedAt: model.Now()})); err != nil {
		t.Fatal(err)
	}
	if err := f.store.View(f.ctx, func(tx *storage.Tx) error {
		c, err := tx.Command(command.ID)
		if err != nil {
			return err
		}
		if c.State != "exited" || c.ExitCode == nil || *c.ExitCode != 7 || !c.OutputIncomplete {
			t.Fatalf("lost exit/capture metadata: %+v", c)
		}
		w, err := tx.Workspace(task.WorkspaceID)
		if err != nil {
			return err
		}
		if w.Quarantined {
			t.Fatalf("known exit quarantined workspace: %+v", w)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	value, err := f.agent("command_output", Args{TaskID: task.ID, CommandID: command.ID, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if !value.(model.OutputResult).OutputIncomplete {
		t.Fatal("output tool omitted capture warning")
	}
	next := f.bash(task, "next", "printf next")
	f.finish(next, "exited")
}
