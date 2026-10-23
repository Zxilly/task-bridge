package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/client"
	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
	"github.com/coder/websocket"
)

func TestSlowConsumerDisconnectPreservesAndReplaysUnacknowledgedOutbox(t *testing.T) {
	f := setup(t)
	f.stopB()
	j, e := client.OpenJournal(f.cfg.JournalPath)
	if e != nil {
		t.Fatal(e)
	}
	stream := j.StreamID()
	j.Close()
	token, e := f.cfg.Token()
	if e != nil {
		t.Fatal(e)
	}
	waitCount := func(want int, limit time.Duration) {
		t.Helper()
		end := time.Now().Add(limit)
		for {
			_, n := f.broker.ConnectionCounts()
			if n == want {
				return
			}
			if time.Now().After(end) {
				t.Fatalf("executor count=%d want=%d", n, want)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitCount(0, 3*time.Second)
	connect := func() (*websocket.Conn, model.Welcome) {
		t.Helper()
		c, _, e := websocket.Dial(f.ctx, f.cfg.ServerURL, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{"Bearer " + token}}})
		if e != nil {
			t.Fatal(e)
		}
		c.SetReadLimit(model.MaxMessageBytes)
		t.Cleanup(func() { c.CloseNow() })
		hello := model.Envelope{Version: 1, Type: "hello", Payload: model.JSON(model.Hello{ExecutorID: "b1", Version: model.Version, Workspaces: []string{"demo"}, StreamID: stream})}
		if e = c.Write(f.ctx, websocket.MessageText, model.JSON(hello)); e != nil {
			t.Fatal(e)
		}
		_, data, e := c.Read(f.ctx)
		if e != nil {
			t.Fatal(e)
		}
		var welcome model.Envelope
		if e = json.Unmarshal(data, &welcome); e != nil || welcome.Type != "welcome" {
			t.Fatalf("welcome: %s %v", data, e)
		}
		var result model.Welcome
		if e = json.Unmarshal(welcome.Payload, &result); e != nil {
			t.Fatal(e)
		}
		return c, result
	}
	slow, _ := connect()
	payload := model.Dispatch{WorkspaceID: "demo", Command: model.Command{ID: "backpressure-probe", Command: strings.Repeat("x", 32<<10)}}
	if e = f.store.Update(f.ctx, func(tx *storage.Tx) error {
		for n := 0; n < 100; n++ {
			if e := tx.Enqueue("b1", "command.dispatch", payload); e != nil {
				return e
			}
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	// This peer deliberately does not read application frames. Queue limits or
	// the bounded writer must disconnect it without acknowledging durable data.
	waitCount(0, 15*time.Second)
	slow.CloseNow()
	var pending int
	if e = f.store.DB.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM outbox WHERE peer='b1' AND acked=0").Scan(&pending); e != nil || pending != 100 {
		t.Fatalf("slow consumer lost durable messages: %d %v", pending, e)
	}
	c, welcome := connect()
	seen := map[string]bool{}
	readCtx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancel()
	for len(seen) < 100 {
		_, data, e := c.Read(readCtx)
		if e != nil {
			t.Fatal("fast consumer replay:", e)
		}
		var msg model.Envelope
		if e = json.Unmarshal(data, &msg); e != nil {
			t.Fatal(e)
		}
		if msg.Type != "command.dispatch" {
			t.Fatalf("unexpected replay type %q", msg.Type)
		}
		seen[msg.ID] = true
		ack := model.Envelope{Version: 1, Type: "ack", RequestID: msg.ID, Seq: msg.Seq, StreamID: msg.StreamID, ServerEpoch: welcome.ServerEpoch, Generation: welcome.Generation}
		if e = c.Write(readCtx, websocket.MessageText, model.JSON(ack)); e != nil {
			t.Fatal(e)
		}
	}
	// Each ACK commits through the same FULL-sync SQLite writer. Verify
	// eventual persistence without imposing a disk-throughput target on race
	// builds or other tests sharing the machine.
	ackStart := time.Now()
	end := ackStart.Add(10 * time.Second)
	for {
		if e = f.store.DB.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM outbox WHERE peer='b1' AND acked=0").Scan(&pending); e != nil {
			t.Fatal(e)
		}
		if pending == 0 {
			break
		}
		if time.Now().After(end) {
			_, connected := f.broker.ConnectionCounts()
			t.Fatalf("ACKs not durable after %s: pending=%d connected=%d", time.Since(ackStart), pending, connected)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("remaining ACKs durably committed after %s", time.Since(ackStart))
	// A healthy reader stays connected after partial ACKs; no replay burst grows
	// behind it while it consumes the remainder of the committed batch.
	_, n := f.broker.ConnectionCounts()
	if n != 1 {
		t.Fatal("healthy replay consumer was disconnected")
	}
}
