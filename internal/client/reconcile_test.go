package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/coder/websocket"
)

func reconcileTestServer(t *testing.T, respond func(int, reconcileArgs) (reconcileResponse, *model.Error)) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	requests := &atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer recovery-token" {
			http.Error(w, "bad token", 401)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		conn.SetReadLimit(model.MaxMessageBytes)
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		if _, _, err = conn.Read(ctx); err != nil {
			return
		}
		send := func(msg model.Envelope) error {
			msg.Version = 1
			msg.ServerEpoch = "new-epoch"
			msg.Generation = 2
			return writeEnvelope(ctx, conn, msg)
		}
		if send(model.Envelope{Type: "welcome", Payload: model.JSON(model.Welcome{ServerEpoch: "new-epoch", Generation: 2, StreamID: "new-server-stream"})}) != nil {
			return
		}
		// An ordinary dispatch may already be queued by the broker. Maintenance
		// must not acknowledge or execute it while waiting for the RPC reply.
		if send(model.Envelope{Type: "command.dispatch", ID: "maintenance-dispatch", Payload: model.JSON(model.Dispatch{WorkspaceID: "local", Command: model.Command{ID: "must-not-run", Command: "printf wrong > maintenance-marker"}})}) != nil {
			return
		}
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var message model.Envelope
			if json.Unmarshal(data, &message) != nil {
				return
			}
			if message.Type != "rpc" {
				t.Errorf("maintenance sent unexpected message %q", message.Type)
				return
			}
			if len(data) > 512<<10 {
				t.Error("reconciliation exceeded bounded wire size")
				return
			}
			var rpc model.RPC
			var args reconcileArgs
			if json.Unmarshal(message.Payload, &rpc) != nil || rpc.Method != "executor_reconcile" || json.Unmarshal(rpc.Args, &args) != nil {
				t.Error("unexpected maintenance RPC")
				return
			}
			if len(args.Records) > 100 {
				t.Error("batch exceeds 100 records")
				return
			}
			response, rpcError := respond(int(requests.Add(1)), args)
			if send(model.Envelope{Type: "response", RequestID: message.RequestID, Payload: model.JSON(response), Error: rpcError}) != nil {
				return
			}
		}
	}))
	return server, requests
}

func reconcileTestConfig(t *testing.T, server *httptest.Server) Config {
	t.Helper()
	t.Setenv("BRIDGE_RECOVERY_TOKEN", "recovery-token")
	return Config{ServerURL: "ws" + strings.TrimPrefix(server.URL, "http"), ExecutorID: "executor", TokenEnv: "BRIDGE_RECOVERY_TOKEN", JournalPath: filepath.Join(t.TempDir(), "journal.db"), Workspaces: map[string]WorkspaceConfig{"local": {Root: t.TempDir(), BashPath: clientTestBash(t)}}}
}

func TestReconcileArchivesOrphansAndReplaysOnlyKnownEvidence(t *testing.T) {
	server, requests := reconcileTestServer(t, func(_ int, args reconcileArgs) (reconcileResponse, *model.Error) {
		if args.Reason != "verified restored server against retained journal" {
			t.Error("recovery reason lost")
		}
		if len(args.Records) != 2 {
			t.Errorf("records=%d", len(args.Records))
		}
		for _, record := range args.Records {
			if record.CommandID == "known" && (record.State != "exited" || record.ExitCode == nil || *record.ExitCode != 7 || record.FinishedAt != 1234 || record.DurationMS != 12) {
				t.Errorf("final evidence changed: %+v", record)
			}
			if record.CommandID == "orphan" && record.State != "unknown" {
				t.Errorf("in-flight command not recovered: %+v", record)
			}
		}
		return reconcileResponse{KnownCommandIDs: []string{"known"}, OrphanCommandIDs: []string{"orphan"}}, nil
	})
	defer server.Close()
	cfg := reconcileTestConfig(t, server)
	j, err := OpenJournal(cfg.JournalPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.SetServerEpoch("old-epoch"); err != nil {
		t.Fatal(err)
	}
	known := model.Dispatch{WorkspaceID: "local", Command: model.Command{ID: "known", TaskID: "task-known", Command: "printf retained", OperationKey: "op-known", TimeoutMS: 1000}}
	orphan := model.Dispatch{WorkspaceID: "local", Command: model.Command{ID: "orphan", TaskID: "lost-task", Command: "sleep 1", OperationKey: "op-orphan", TimeoutMS: 1000}}
	for _, d := range []model.Dispatch{known, orphan} {
		if _, _, err = j.Admit("dispatch-"+d.Command.ID, d); err != nil {
			t.Fatal(err)
		}
	}
	for _, chunk := range []model.Chunk{{CommandID: "known", Seq: 2, Stream: "stderr", Data: []byte("two")}, {CommandID: "known", Seq: 1, Stream: "stdout", Data: []byte("one")}, {CommandID: "orphan", Seq: 1, Stream: "stdout", Data: []byte{0, 255, 1}}} {
		if err = j.Record("command.output", model.CommandEvent{CommandID: chunk.CommandID, Chunk: &chunk}, 0); err != nil {
			t.Fatal(err)
		}
	}
	code := 7
	if err = j.Record("command.finished", model.CommandEvent{CommandID: "known", State: "exited", ExitCode: &code, FinishedAt: 1234, DurationMS: 12}, 0); err != nil {
		t.Fatal(err)
	}
	if err = j.Record("command.started", model.CommandEvent{CommandID: "orphan", State: "running"}, 123); err != nil {
		t.Fatal(err)
	}
	if _, err = Reconcile(context.Background(), cfg, "active journal must reject maintenance"); err == nil {
		t.Fatal("maintenance acquired a running client's journal")
	}
	if requests.Load() != 0 {
		t.Fatal("locked journal connected to server")
	}
	j.Close()
	result, err := Reconcile(context.Background(), cfg, "verified restored server against retained journal")
	if err != nil {
		t.Fatal(err)
	}
	if result.OldEpoch != "old-epoch" || result.NewEpoch != "new-epoch" || result.KnownCommands != 1 || result.OrphanCommands != 1 || result.UnknownCommands != 1 || result.ArchivedEvents != 8 || result.ReplayedEvents != 3 {
		t.Fatalf("result: %+v", result)
	}
	j, err = OpenJournal(cfg.JournalPath)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	epoch, err := j.serverEpoch()
	if err != nil || epoch != "new-epoch" {
		t.Fatalf("epoch=%q error=%v", epoch, err)
	}
	pending, err := j.Pending(100)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 3 {
		t.Fatalf("pending=%d", len(pending))
	}
	for index, message := range pending {
		var event model.CommandEvent
		if err = json.Unmarshal(message.Payload, &event); err != nil {
			t.Fatal(err)
		}
		if event.CommandID != "known" {
			t.Fatal("orphan entered ordinary replay")
		}
		if index < 2 && (event.Chunk == nil || event.Chunk.Seq != int64(index+1)) {
			t.Fatalf("chunks not replayed in sequence: %+v", event)
		}
		if index == 2 && (message.Type != "command.finished" || event.State != "exited") {
			t.Fatalf("missing final result: %+v", event)
		}
	}
	var archived, retained int
	if err = j.db.QueryRow(`SELECT count(*) FROM reconciliation_pending`).Scan(&archived); err != nil || archived != 8 {
		t.Fatalf("archive=%d %v", archived, err)
	}
	if err = j.db.QueryRow(`SELECT count(*) FROM chunks WHERE command_id='orphan'`).Scan(&retained); err != nil || retained != 1 {
		t.Fatalf("orphan output lost: %d %v", retained, err)
	}
	if fresh, _, err := j.Admit("orphan-replay", orphan); err != nil || fresh {
		t.Fatalf("orphan lost dedup: %v %v", fresh, err)
	}
	if _, err = os.Stat(filepath.Join(cfg.Workspaces["local"].Root, "maintenance-marker")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("maintenance executed dispatch")
	}
}

func TestReconcileLaterBatchFailureLeavesEpochAndPendingUnchanged(t *testing.T) {
	server, requests := reconcileTestServer(t, func(batch int, args reconcileArgs) (reconcileResponse, *model.Error) {
		if batch == 2 {
			return reconcileResponse{}, &model.Error{Code: "terminal_conflict", Message: "review required"}
		}
		response := reconcileResponse{}
		for _, record := range args.Records {
			response.KnownCommandIDs = append(response.KnownCommandIDs, record.CommandID)
		}
		return response, nil
	})
	defer server.Close()
	cfg := reconcileTestConfig(t, server)
	j, err := OpenJournal(cfg.JournalPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.SetServerEpoch("old-epoch"); err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 101; n++ {
		id := fmt.Sprintf("command-%03d", n)
		if _, _, err = j.Admit("dispatch-"+id, model.Dispatch{WorkspaceID: "local", Command: model.Command{ID: id, TaskID: "task", Command: "true", OperationKey: id}}); err != nil {
			t.Fatal(err)
		}
		if err = j.Record("command.finished", model.CommandEvent{CommandID: id, State: "not_started", FinishedAt: 1234}, 0); err != nil {
			t.Fatal(err)
		}
	}
	before, err := j.Pending(1000)
	if err != nil {
		t.Fatal(err)
	}
	j.Close()
	if _, err = Reconcile(context.Background(), cfg, "verified recovery but one conflicting result"); err == nil {
		t.Fatal("accepted failed reconciliation batch")
	}
	if requests.Load() != 2 {
		t.Fatalf("batches=%d", requests.Load())
	}
	j, err = OpenJournal(cfg.JournalPath)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	epoch, err := j.serverEpoch()
	if err != nil || epoch != "old-epoch" {
		t.Fatalf("epoch advanced before all batches: %q %v", epoch, err)
	}
	after, err := j.Pending(1000)
	if err != nil {
		t.Fatal(err)
	}
	if string(model.JSON(before)) != string(model.JSON(after)) {
		t.Fatal("failed reconciliation changed pending evidence")
	}
	var history int
	if err = j.db.QueryRow(`SELECT count(*) FROM reconciliation_history`).Scan(&history); err != nil || history != 0 {
		t.Fatalf("committed failed attempt: %d %v", history, err)
	}
}

func TestReconcileRejectsIncompleteOrInventedClassifications(t *testing.T) {
	records := []reconcileRecord{{CommandID: "one"}, {CommandID: "two"}}
	for _, response := range []reconcileResponse{{KnownCommandIDs: []string{"one"}}, {KnownCommandIDs: []string{"one", "two"}, OrphanCommandIDs: []string{"one"}}, {KnownCommandIDs: []string{"one", "foreign"}}} {
		if _, err := validateReconcileResponse(records, response); err == nil {
			t.Fatalf("accepted invalid response: %+v", response)
		}
	}
}

func TestReconcileAdoptsVerifiedServerResultAndPreservesOriginalUnknown(t *testing.T) {
	code := 0
	verified := model.CommandEvent{CommandID: "known", State: "exited", ExitCode: &code, FinishedAt: 5678, DurationMS: 52}
	server, _ := reconcileTestServer(t, func(_ int, args reconcileArgs) (reconcileResponse, *model.Error) {
		if len(args.Records) != 1 || args.Records[0].State != "unknown" {
			t.Errorf("original uncertainty was not submitted: %+v", args.Records)
		}
		return reconcileResponse{KnownCommandIDs: []string{"known"}, ServerResults: map[string]model.CommandEvent{"known": verified}}, nil
	})
	defer server.Close()
	cfg := reconcileTestConfig(t, server)
	j, err := OpenJournal(cfg.JournalPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.SetServerEpoch("old-epoch"); err != nil {
		t.Fatal(err)
	}
	if _, _, err = j.Admit("dispatch", model.Dispatch{WorkspaceID: "local", Command: model.Command{ID: "known", TaskID: "task", Command: "true", OperationKey: "op"}}); err != nil {
		t.Fatal(err)
	}
	if err = j.Record("command.output", model.CommandEvent{CommandID: "known", Chunk: &model.Chunk{CommandID: "known", Seq: 1, Stream: "stdout", Data: []byte("original evidence")}}, 0); err != nil {
		t.Fatal(err)
	}
	original := model.CommandEvent{CommandID: "known", State: "unknown", FinishedAt: 1234}
	if err = j.Record("command.finished", original, 0); err != nil {
		t.Fatal(err)
	}
	// An older broker may have ACKed the unknown event before restoration. The
	// local command and output evidence must still be archived before adoption.
	messages, err := j.Pending(100)
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range messages {
		if err = j.Ack(message.ID); err != nil {
			t.Fatal(err)
		}
	}
	j.Close()
	result, err := Reconcile(context.Background(), cfg, "operator verified the persisted server final result")
	if err != nil {
		t.Fatal(err)
	}
	if result.AdoptedServerResults != 1 || result.UnknownCommands != 0 || result.ArchivedEvents != 0 {
		t.Fatalf("result: %+v", result)
	}
	j, err = OpenJournal(cfg.JournalPath)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err = j.Recover(); err != nil {
		t.Fatal(err)
	}
	var state string
	var data []byte
	if err = j.db.QueryRow(`SELECT state,result FROM commands WHERE id='known'`).Scan(&state, &data); err != nil {
		t.Fatal(err)
	}
	if state != "exited" || string(data) != string(model.JSON(verified)) {
		t.Fatalf("server fact not adopted durably: %s %s", state, data)
	}
	if err = j.db.QueryRow(`SELECT state,result FROM reconciliation_evidence WHERE command_id='known'`).Scan(&state, &data); err != nil {
		t.Fatal(err)
	}
	if state != "unknown" || string(data) != string(model.JSON(original)) {
		t.Fatalf("original uncertainty was overwritten: %s %s", state, data)
	}
	pending, err := j.Pending(100)
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range pending {
		if err = j.Ack(message.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = j.db.Exec(`UPDATE commands SET finished_acked_at=1 WHERE id='known'`); err != nil {
		t.Fatal(err)
	}
	if err = j.PruneOutputs(model.Now()); err != nil {
		t.Fatal(err)
	}
	var archived int
	if err = j.db.QueryRow(`SELECT count(*) FROM reconciliation_chunks WHERE command_id='known' AND data=?`, []byte("original evidence")).Scan(&archived); err != nil || archived != 1 {
		t.Fatalf("cleanup removed original unknown output: %d %v", archived, err)
	}
}

func TestReconcileRejectsInvalidAuthoritativeServerResults(t *testing.T) {
	code := 0
	cases := []struct {
		local string
		known bool
		key   string
		event model.CommandEvent
	}{
		{"unknown", true, "one", model.CommandEvent{CommandID: "one", State: "unknown"}},
		{"unknown", true, "one", model.CommandEvent{CommandID: "wrong", State: "exited", ExitCode: &code}},
		{"unknown", true, "one", model.CommandEvent{CommandID: "one", State: "exited"}},
		{"unknown", true, "foreign", model.CommandEvent{CommandID: "foreign", State: "cancelled"}},
		{"unknown", false, "one", model.CommandEvent{CommandID: "one", State: "cancelled"}},
		{"exited", true, "one", model.CommandEvent{CommandID: "one", State: "cancelled"}},
		{"unknown", true, "one", model.CommandEvent{CommandID: "one", State: "running"}},
		{"unknown", true, "one", model.CommandEvent{CommandID: "one", State: "cancelled", Chunk: &model.Chunk{Data: []byte("injected")}}},
	}
	for index, test := range cases {
		response := reconcileResponse{ServerResults: map[string]model.CommandEvent{test.key: test.event}}
		if test.known {
			response.KnownCommandIDs = []string{"one"}
		} else {
			response.OrphanCommandIDs = []string{"one"}
		}
		if _, err := validateReconcileResponse([]reconcileRecord{{CommandID: "one", State: test.local}}, response); err == nil {
			t.Errorf("case %d accepted invalid server result", index)
		}
	}
}

func TestReconcileFinalTransactionRollbackPreservesOriginalEvidence(t *testing.T) {
	j, err := OpenJournal(filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err = j.SetServerEpoch("old-epoch"); err != nil {
		t.Fatal(err)
	}
	if _, _, err = j.Admit("dispatch", model.Dispatch{WorkspaceID: "local", Command: model.Command{ID: "one", Command: "true"}}); err != nil {
		t.Fatal(err)
	}
	if err = j.Record("command.finished", model.CommandEvent{CommandID: "one", State: "unknown", FinishedAt: 1234}, 0); err != nil {
		t.Fatal(err)
	}
	before, err := j.Pending(100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = j.db.Exec(`CREATE TRIGGER reject_epoch BEFORE INSERT ON metadata WHEN NEW.key='server_epoch' BEGIN SELECT RAISE(FAIL,'injected epoch write failure'); END`); err != nil {
		t.Fatal(err)
	}
	_, err = j.commitReconciliation("new-epoch", "failure injection", []reconciliationOutcome{{id: "one", known: true, serverResult: &model.CommandEvent{CommandID: "one", State: "cancelled", FinishedAt: 5678}}})
	if err == nil {
		t.Fatal("fault injection did not reject commit")
	}
	if epoch, err := j.serverEpoch(); err != nil || epoch != "old-epoch" {
		t.Fatalf("failed transaction changed epoch: %q %v", epoch, err)
	}
	after, err := j.Pending(100)
	if err != nil {
		t.Fatal(err)
	}
	if string(model.JSON(before)) != string(model.JSON(after)) {
		t.Fatal("failed final commit replaced pending events")
	}
	var state string
	if err = j.db.QueryRow(`SELECT state FROM commands WHERE id='one'`).Scan(&state); err != nil || state != "unknown" {
		t.Fatalf("failed commit adopted new state: %s %v", state, err)
	}
	var count int
	if err = j.db.QueryRow(`SELECT count(*) FROM reconciliation_history`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed commit left partial history: %d %v", count, err)
	}
}

func TestReconcileOrphanOutputCannotExpireUsingOldEpochAcknowledgement(t *testing.T) {
	j, err := OpenJournal(filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err = j.SetServerEpoch("old-epoch"); err != nil {
		t.Fatal(err)
	}
	if _, _, err = j.Admit("dispatch", model.Dispatch{WorkspaceID: "local", Command: model.Command{ID: "orphan", Command: "true"}}); err != nil {
		t.Fatal(err)
	}
	if err = j.Record("command.output", model.CommandEvent{CommandID: "orphan", Chunk: &model.Chunk{CommandID: "orphan", Seq: 1, Stream: "stdout", Data: []byte("only remaining output")}}, 0); err != nil {
		t.Fatal(err)
	}
	code := 0
	if err = j.Record("command.finished", model.CommandEvent{CommandID: "orphan", State: "exited", ExitCode: &code, FinishedAt: 1234}, 0); err != nil {
		t.Fatal(err)
	}
	messages, err := j.Pending(100)
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range messages {
		if err = j.Ack(message.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = j.db.Exec(`UPDATE commands SET finished_acked_at=1`); err != nil {
		t.Fatal(err)
	}
	if _, err = j.commitReconciliation("new-epoch", "old snapshot lacks this command", []reconciliationOutcome{{id: "orphan", known: false}}); err != nil {
		t.Fatal(err)
	}
	if err = j.PruneOutputs(model.Now()); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = j.db.QueryRow(`SELECT count(*) FROM chunks WHERE command_id='orphan'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("orphan output expired using stale ACK: %d %v", count, err)
	}
}

func TestChangedEpochDoesNotPruneRetainedOutputBeforeReconciliation(t *testing.T) {
	server, _ := reconcileTestServer(t, func(_ int, _ reconcileArgs) (reconcileResponse, *model.Error) {
		t.Error("ordinary client called reconciliation")
		return reconcileResponse{}, nil
	})
	defer server.Close()
	cfg := reconcileTestConfig(t, server)
	j, err := OpenJournal(cfg.JournalPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.SetServerEpoch("old-epoch"); err != nil {
		t.Fatal(err)
	}
	if _, _, err = j.Admit("dispatch", model.Dispatch{WorkspaceID: "local", Command: model.Command{ID: "known", Command: "true"}}); err != nil {
		t.Fatal(err)
	}
	if err = j.Record("command.output", model.CommandEvent{CommandID: "known", Chunk: &model.Chunk{CommandID: "known", Seq: 1, Stream: "stdout", Data: []byte("retained restored-server evidence")}}, 0); err != nil {
		t.Fatal(err)
	}
	code := 0
	if err = j.Record("command.finished", model.CommandEvent{CommandID: "known", State: "exited", ExitCode: &code}, 0); err != nil {
		t.Fatal(err)
	}
	messages, err := j.Pending(100)
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range messages {
		if err = j.Ack(message.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = j.db.Exec(`UPDATE commands SET finished_acked_at=1`); err != nil {
		t.Fatal(err)
	}
	j.Close()
	c, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = c.Run(ctx); err == nil || !strings.Contains(err.Error(), "epoch changed") {
		t.Fatalf("normal client accepted changed epoch: %v", err)
	}
	var count int
	if err = c.journal.db.QueryRow(`SELECT count(*) FROM chunks WHERE command_id='known'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("epoch check deleted retained evidence: %d %v", count, err)
	}
}

func TestJournalMismatchStopsOrdinaryClientWithoutReconnectLoop(t *testing.T) {
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if _, _, err = conn.Read(ctx); err != nil {
			return
		}
		_ = writeEnvelope(ctx, conn, model.Envelope{Version: 1, Type: "error", Error: &model.Error{Code: "journal_mismatch", Message: "retain the enrolled journal"}})
	}))
	defer server.Close()
	cfg := reconcileTestConfig(t, server)
	c, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = c.Run(ctx)
	var domain *model.Error
	if !errors.As(err, &domain) || domain.Code != "journal_mismatch" {
		t.Fatalf("journal mismatch did not stop client: %v", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("journal mismatch retried %d times", attempts.Load())
	}
}
