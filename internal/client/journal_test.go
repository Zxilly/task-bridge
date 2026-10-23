package client

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
)

func TestJournalDeduplicatesAndRecoversUnknown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := OpenJournal(path); err == nil {
		other.Close()
		t.Fatal("second client acquired journal")
	}
	dispatch := model.Dispatch{WorkspaceID: "local", Command: model.Command{ID: "command-1", TaskID: "task-1", Command: "printf effect"}}
	fresh, _, err := j.Admit("message-1", dispatch)
	if err != nil || !fresh {
		t.Fatalf("admit: %v %v", fresh, err)
	}
	fresh, _, err = j.Admit("message-1", dispatch)
	if err != nil || fresh {
		t.Fatalf("duplicate: %v %v", fresh, err)
	}
	stream := j.StreamID()
	j.Close()
	j, err = OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if j.StreamID() != stream {
		t.Fatal("reconnect changed durable stream identity")
	}
	if err = j.Recover(); err != nil {
		t.Fatal(err)
	}
	pending, err := j.Pending(100)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, msg := range pending {
		if msg.Type == "command.finished" {
			var e model.CommandEvent
			if err = json.Unmarshal(msg.Payload, &e); err != nil {
				t.Fatal(err)
			}
			if e.State == "unknown" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("recovery did not preserve ambiguous execution")
	}
	fresh, _, err = j.Admit("replayed", dispatch)
	if err != nil || fresh {
		t.Fatalf("unknown command scheduled again: %v %v", fresh, err)
	}
}

func TestJournalCancelBeforeDispatch(t *testing.T) {
	j, err := OpenJournal(filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err = j.Cancel("cancel-1", "command-1"); err != nil {
		t.Fatal(err)
	}
	fresh, cancelled, err := j.Admit("dispatch-1", model.Dispatch{WorkspaceID: "local", Command: model.Command{ID: "command-1", Command: "exit"}})
	if err != nil || fresh || !cancelled {
		t.Fatalf("cancelled dispatch admitted: %v %v %v", fresh, cancelled, err)
	}
	pending, err := j.Pending(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Type != "command.finished" {
		t.Fatalf("pending: %+v", pending)
	}
	var persisted []byte
	if err = j.db.QueryRow(`SELECT result FROM commands WHERE id='command-1'`).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if string(persisted) != string(pending[0].Payload) {
		t.Fatal("cancelled-before-dispatch result is not durable for later reconciliation")
	}
}

func TestJournalEpochAndAcknowledgements(t *testing.T) {
	j, err := OpenJournal(filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err = j.SetServerEpoch("epoch-1"); err != nil {
		t.Fatal(err)
	}
	if err = j.SetServerEpoch("epoch-1"); err != nil {
		t.Fatal(err)
	}
	if err = j.SetServerEpoch("restored-backup"); err == nil {
		t.Fatal("accepted data rollback without reconciliation")
	}
	if err = j.Record("command.finished", model.CommandEvent{CommandID: "command-1", State: "exited"}, 0); err != nil {
		t.Fatal(err)
	}
	messages, err := j.Pending(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Ack(messages[0].ID); err != nil {
		t.Fatal(err)
	}
	if err = j.Ack(messages[0].ID); err != nil {
		t.Fatal(err)
	}
	messages, err = j.Pending(1)
	if err != nil || len(messages) != 0 {
		t.Fatalf("ack did not retire pending event: %v %v", messages, err)
	}
}

func TestJournalPrunesOnlyAcknowledgedFinalOutput(t *testing.T) {
	j, err := OpenJournal(filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	for _, id := range []string{"done", "unacked", "unknown"} {
		if _, _, err = j.Admit("dispatch-"+id, model.Dispatch{WorkspaceID: "local", Command: model.Command{ID: id, Command: "echo result"}}); err != nil {
			t.Fatal(err)
		}
		if err = j.Record("command.output", model.CommandEvent{CommandID: id, Chunk: &model.Chunk{CommandID: id, Seq: 1, Stream: "stdout", Data: []byte("evidence")}}, 0); err != nil {
			t.Fatal(err)
		}
		state := "exited"
		if id == "unknown" {
			state = "unknown"
		}
		if err = j.Record("command.finished", model.CommandEvent{CommandID: id, State: state, FinishedAt: model.Now()}, 0); err != nil {
			t.Fatal(err)
		}
	}
	pending, err := j.Pending(100)
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range pending {
		var event model.CommandEvent
		if err = json.Unmarshal(message.Payload, &event); err != nil {
			t.Fatal(err)
		}
		if event.CommandID != "unacked" {
			if err = j.Ack(message.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err = j.db.Exec(`UPDATE commands SET finished_acked_at=? WHERE finished_acked_at>0`, time.Now().Add(-4*24*time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err = j.PruneOutputs(time.Now().Add(-3 * 24 * time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"done", "unacked", "unknown"} {
		var count int
		if err = j.db.QueryRow(`SELECT count(*) FROM chunks WHERE command_id=?`, id).Scan(&count); err != nil {
			t.Fatal(err)
		}
		expected := 1
		if id == "done" {
			expected = 0
		}
		if count != expected {
			t.Fatalf("%s retained %d chunks, want %d", id, count, expected)
		}
		fresh, _, err := j.Admit("replay-"+id, model.Dispatch{WorkspaceID: "local", Command: model.Command{ID: id, Command: "echo result"}})
		if err != nil || fresh {
			t.Fatalf("cleanup removed dedup for %s: %v %v", id, fresh, err)
		}
	}
}

func TestJournalCapacityPreservesTerminalWritesAndDedup(t *testing.T) {
	j, err := OpenJournal(filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	dispatch := model.Dispatch{WorkspaceID: "local", Command: model.Command{ID: "running", Command: "echo result"}}
	if _, _, err = j.Admit("dispatch", dispatch); err != nil {
		t.Fatal(err)
	}
	j.maxBytes = 16 << 20 // All remaining capacity is reserved for durable terminal state.
	if err = j.Capacity(); err == nil {
		t.Fatal("capacity limit ignored")
	}
	if err = j.Record("command.output", model.CommandEvent{CommandID: "running", Chunk: &model.Chunk{CommandID: "running", Seq: 1, Stream: "stdout", Data: []byte("overflow")}}, 0); err == nil {
		t.Fatal("accepted output after capacity limit")
	}
	if err = j.Record("command.finished", model.CommandEvent{CommandID: "running", State: "unknown", FinishedAt: model.Now()}, 0); err != nil {
		t.Fatal(err)
	}
	if fresh, _, err := j.Admit("replay", dispatch); err != nil || fresh {
		t.Fatalf("duplicate handling failed at capacity: %v %v", fresh, err)
	}
	dispatch.Command.ID = "new"
	if _, _, err = j.Admit("dispatch-new", dispatch); err == nil {
		t.Fatal("accepted new command at capacity")
	}
}
