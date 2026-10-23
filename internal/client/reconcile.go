package client

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/coder/websocket"
	"github.com/google/uuid"
)

type reconcileRecord struct {
	CommandID    string       `json:"command_id"`
	TaskID       string       `json:"task_id"`
	WorkspaceID  string       `json:"workspace_id"`
	State        string       `json:"state"`
	Command      string       `json:"command"`
	OperationKey string       `json:"operation_key"`
	Cwd          string       `json:"cwd,omitempty"`
	TimeoutMS    int64        `json:"timeout_ms"`
	ExitCode     *int         `json:"exit_code,omitempty"`
	Signal       string       `json:"signal,omitempty"`
	FinishedAt   int64        `json:"finished_at,omitempty"`
	DurationMS   int64        `json:"duration_ms,omitempty"`
	Error        *model.Error `json:"error,omitempty"`
}
type reconcileArgs struct {
	Reason  string            `json:"reason"`
	Records []reconcileRecord `json:"records"`
}
type reconcileResponse struct {
	KnownCommandIDs  []string                      `json:"known_command_ids"`
	OrphanCommandIDs []string                      `json:"orphan_command_ids"`
	ServerResults    map[string]model.CommandEvent `json:"server_results,omitempty"`
}
type reconciliationOutcome struct {
	id           string
	known        bool
	serverResult *model.CommandEvent
}

// ReconcileResult describes evidence exchanged with the broker. Unknown local
// executions and workspace quarantine always require separate operator review.
type ReconcileResult struct {
	ReconciliationID     string `json:"reconciliation_id"`
	OldEpoch             string `json:"old_epoch"`
	NewEpoch             string `json:"new_epoch"`
	KnownCommands        int    `json:"known_commands"`
	OrphanCommands       int    `json:"orphan_commands"`
	UnknownCommands      int    `json:"unknown_commands"`
	AdoptedServerResults int    `json:"adopted_server_results"`
	ArchivedEvents       int64  `json:"archived_events"`
	ReplayedEvents       int64  `json:"replayed_events"`
	Message              string `json:"message"`
}

// Reconcile exclusively opens the journal and permits an explicit epoch change.
// No dispatch is acknowledged or executed by this connection. The old epoch and
// pending events remain in place unless every reconciliation batch is confirmed.
func Reconcile(ctx context.Context, cfg Config, reason string) (ReconcileResult, error) {
	var result ReconcileResult
	reason = strings.TrimSpace(reason)
	if reason == "" || len(reason) > 2048 {
		return result, errors.New("reconciliation requires a reason containing 1 to 2048 bytes of reviewed evidence")
	}
	if err := cfg.Validate(); err != nil {
		return result, err
	}
	token, err := cfg.Token()
	if err != nil {
		return result, err
	}
	if _, err = os.Stat(cfg.JournalPath); err != nil {
		return result, fmt.Errorf("reconciliation requires the existing retained journal: %w", err)
	}
	j, err := OpenJournal(cfg.JournalPath)
	if err != nil {
		return result, err
	}
	defer j.Close()
	if err = j.Recover(); err != nil {
		return result, err
	}
	if result.OldEpoch, err = j.serverEpoch(); err != nil {
		return result, err
	}
	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	conn, _, err := websocket.Dial(dialCtx, cfg.ServerURL, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{"Bearer " + token}}})
	cancel()
	if err != nil {
		return result, err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(model.MaxMessageBytes)
	workspaces := make([]string, 0, len(cfg.Workspaces))
	for id := range cfg.Workspaces {
		workspaces = append(workspaces, id)
	}
	sort.Strings(workspaces)
	hello := model.Envelope{Version: model.ProtocolVersion, Type: "hello", Payload: model.JSON(model.Hello{ExecutorID: cfg.ExecutorID, Version: model.Version, Workspaces: workspaces, StreamID: j.StreamID()})}
	if err = writeEnvelope(ctx, conn, hello); err != nil {
		return result, err
	}
	handshakeCtx, endHandshake := context.WithTimeout(ctx, 15*time.Second)
	_, data, err := conn.Read(handshakeCtx)
	endHandshake()
	if err != nil {
		return result, err
	}
	var envelope model.Envelope
	if err = json.Unmarshal(data, &envelope); err != nil {
		return result, err
	}
	if envelope.Error != nil {
		return result, envelope.Error
	}
	if envelope.Version != model.ProtocolVersion || envelope.Type != "welcome" {
		return result, errors.New("reconciliation expected a compatible broker welcome")
	}
	var welcome model.Welcome
	if err = json.Unmarshal(envelope.Payload, &welcome); err != nil {
		return result, err
	}
	if welcome.ServerEpoch == "" || welcome.Generation <= 0 {
		return result, errors.New("broker welcome lacks epoch or generation")
	}
	result.NewEpoch = welcome.ServerEpoch
	var outcomes []reconciliationOutcome
	after := ""
	for {
		records, err := j.reconcileRecords(after, 100)
		if err != nil {
			return result, err
		}
		if len(records) == 0 && after != "" {
			break
		}
		// Keep both the record count and wire size bounded. Empty journals still
		// send one batch so the server audits this explicit recovery operation.
		for len(records) > 0 || after == "" {
			count := len(records)
			for count > 1 && len(model.JSON(reconcileArgs{Reason: reason, Records: records[:count]})) > 480<<10 {
				count /= 2
			}
			batch := records[:count]
			if batch == nil {
				batch = []reconcileRecord{}
			}
			if len(model.JSON(reconcileArgs{Reason: reason, Records: batch})) > 480<<10 {
				return result, errors.New("one reconciliation record exceeds the bounded recovery message size")
			}
			response, err := reconcileRPC(ctx, conn, welcome, reconcileArgs{Reason: reason, Records: batch})
			if err != nil {
				return result, err
			}
			classified, err := validateReconcileResponse(batch, response)
			if err != nil {
				return result, err
			}
			outcomes = append(outcomes, classified...)
			for _, record := range batch {
				if record.State == "unknown" {
					result.UnknownCommands++
				}
			}
			if count == 0 {
				after = "\x00"
				break
			}
			after = batch[count-1].CommandID
			records = records[count:]
		}
		if after == "\x00" {
			break
		}
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	committed, err := j.commitReconciliation(welcome.ServerEpoch, reason, outcomes)
	if err != nil {
		return result, err
	}
	committed.UnknownCommands = result.UnknownCommands - committed.AdoptedServerResults
	committed.Message = "Journal epoch reconciled. Known evidence will replay on the next mcp connection; orphan evidence remains archived locally. Verified server terminal results were adopted only after archiving the original unknown evidence. No command was executed or old PID killed; remaining unknown states and workspace quarantine require operator review."
	return committed, nil
}

func reconcileRPC(ctx context.Context, conn *websocket.Conn, welcome model.Welcome, args reconcileArgs) (reconcileResponse, error) {
	var response reconcileResponse
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	id := uuid.NewString()
	request := model.Envelope{Version: model.ProtocolVersion, Type: "rpc", RequestID: id, ServerEpoch: welcome.ServerEpoch, Generation: welcome.Generation, Payload: model.JSON(model.RPC{Method: "executor_reconcile", Args: model.JSON(args)})}
	if err := writeEnvelope(ctx, conn, request); err != nil {
		return response, err
	}
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return response, err
		}
		var message model.Envelope
		if err = json.Unmarshal(data, &message); err != nil {
			return response, err
		}
		if message.Version != model.ProtocolVersion || (message.ServerEpoch != "" && message.ServerEpoch != welcome.ServerEpoch) || (message.Generation != 0 && message.Generation != welcome.Generation) {
			return response, errors.New("broker connection changed during reconciliation")
		}
		if message.Type == "error" && message.Error != nil {
			return response, message.Error
		}
		if message.Type == "response" && message.RequestID == id {
			if message.Error != nil {
				return response, message.Error
			}
			if err = json.Unmarshal(message.Payload, &response); err != nil {
				return response, err
			}
			return response, nil
		}
		if message.Type == "server.draining" {
			return response, errors.New("broker is draining")
		}
		// Dispatch/cancel replay is deliberately left unacknowledged. Only the
		// normal executor may process it after this journal transaction commits.
	}
}

func validateReconcileResponse(records []reconcileRecord, response reconcileResponse) ([]reconciliationOutcome, error) {
	expected := make(map[string]bool, len(records))
	states := make(map[string]string, len(records))
	for _, record := range records {
		expected[record.CommandID] = true
		states[record.CommandID] = record.State
	}
	outcomes := make([]reconciliationOutcome, 0, len(records))
	add := func(ids []string, known bool) error {
		for _, id := range ids {
			if !expected[id] {
				return fmt.Errorf("reconciliation response repeats or introduces command %q", id)
			}
			delete(expected, id)
			outcomes = append(outcomes, reconciliationOutcome{id: id, known: known})
		}
		return nil
	}
	if err := add(response.KnownCommandIDs, true); err != nil {
		return nil, err
	}
	if err := add(response.OrphanCommandIDs, false); err != nil {
		return nil, err
	}
	if len(expected) != 0 {
		return nil, errors.New("reconciliation response omitted commands; journal epoch was not changed")
	}
	for id, event := range response.ServerResults {
		if event.CommandID != id || event.Chunk != nil || states[id] != "unknown" || event.State == "unknown" || !(&model.Command{State: event.State}).Terminal() || (event.State == "exited" && event.ExitCode == nil) || (event.Error != nil && (len(event.Error.Code) > 64 || len(event.Error.Message) > 2048)) {
			return nil, fmt.Errorf("invalid authoritative server result for command %q", id)
		}
		matched := false
		for index := range outcomes {
			if outcomes[index].id == id && outcomes[index].known {
				copy := event
				outcomes[index].serverResult = &copy
				matched = true
				break
			}
		}
		if !matched {
			return nil, fmt.Errorf("server result is outside known commands: %q", id)
		}
	}
	return outcomes, nil
}

func (j *Journal) serverEpoch() (string, error) {
	var epoch string
	err := j.db.QueryRow(`SELECT value FROM metadata WHERE key='server_epoch'`).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return epoch, err
}
func (j *Journal) reconcileRecords(after string, limit int) ([]reconcileRecord, error) {
	rows, err := j.db.Query(`SELECT id,dispatch,state,result FROM commands WHERE id>? ORDER BY id LIMIT ?`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []reconcileRecord
	for rows.Next() {
		var id, state string
		var dispatchData, resultData []byte
		if err = rows.Scan(&id, &dispatchData, &state, &resultData); err != nil {
			return nil, err
		}
		var dispatch model.Dispatch
		var event model.CommandEvent
		if err = json.Unmarshal(dispatchData, &dispatch); err != nil {
			return nil, err
		}
		if len(resultData) > 0 {
			if err = json.Unmarshal(resultData, &event); err != nil {
				return nil, err
			}
		}
		if !(&model.Command{State: state}).Terminal() {
			return nil, fmt.Errorf("command %s is not recovered to a terminal local state", id)
		}
		requestedCwd := dispatch.Command.RequestedCwd
		if requestedCwd == "" {
			requestedCwd = dispatch.Command.Cwd
		}
		records = append(records, reconcileRecord{CommandID: id, TaskID: dispatch.Command.TaskID, WorkspaceID: dispatch.WorkspaceID, State: state, Command: dispatch.Command.Command, OperationKey: dispatch.Command.OperationKey, Cwd: requestedCwd, TimeoutMS: dispatch.Command.TimeoutMS, ExitCode: event.ExitCode, Signal: event.Signal, FinishedAt: event.FinishedAt, DurationMS: event.DurationMS, Error: event.Error})
	}
	return records, rows.Err()
}

func (j *Journal) commitReconciliation(epoch, reason string, outcomes []reconciliationOutcome) (ReconcileResult, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	result := ReconcileResult{ReconciliationID: uuid.NewString(), NewEpoch: epoch}
	tx, err := j.db.Begin()
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	err = tx.QueryRow(`SELECT value FROM metadata WHERE key='server_epoch'`).Scan(&result.OldEpoch)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return result, err
	}
	for _, outcome := range outcomes {
		if outcome.known {
			result.KnownCommands++
		} else {
			result.OrphanCommands++
		}
	}
	if _, err = tx.Exec(`INSERT INTO reconciliation_history(id,old_epoch,new_epoch,reason,created_at,known_count,orphan_count) VALUES(?,?,?,?,?,?,?)`, result.ReconciliationID, result.OldEpoch, epoch, reason, model.Now(), result.KnownCommands, result.OrphanCommands); err != nil {
		return result, err
	}
	archived, err := tx.Exec(`INSERT INTO reconciliation_pending(reconciliation_id,seq,message_id,type,payload) SELECT ?,seq,message_id,type,payload FROM pending`, result.ReconciliationID)
	if err != nil {
		return result, err
	}
	result.ArchivedEvents, _ = archived.RowsAffected()
	if _, err = tx.Exec(`INSERT INTO reconciliation_evidence(reconciliation_id,command_id,dispatch,state,pid,result,finished_acked_at) SELECT ?,id,dispatch,state,pid,result,finished_acked_at FROM commands`, result.ReconciliationID); err != nil {
		return result, err
	}
	if _, err = tx.Exec(`DELETE FROM pending`); err != nil {
		return result, err
	}
	for _, outcome := range outcomes {
		kind := "orphan"
		if outcome.known {
			kind = "known"
		}
		if _, err = tx.Exec(`INSERT INTO reconciliation_records(reconciliation_id,command_id,outcome) VALUES(?,?,?)`, result.ReconciliationID, outcome.id, kind); err != nil {
			return result, err
		}
		// An ACK from the old epoch cannot authorize deleting orphan output.
		// Known commands become eligible again only after the new broker ACKs.
		if _, err = tx.Exec(`UPDATE commands SET finished_acked_at=0 WHERE id=?`, outcome.id); err != nil {
			return result, err
		}
		if !outcome.known {
			continue
		}
		if outcome.serverResult != nil {
			if _, err = tx.Exec(`INSERT INTO reconciliation_server_results(reconciliation_id,command_id,result) VALUES(?,?,?)`, result.ReconciliationID, outcome.id, model.JSON(outcome.serverResult)); err != nil {
				return result, err
			}
			if _, err = tx.Exec(`INSERT INTO reconciliation_chunks(reconciliation_id,command_id,seq,stream,data) SELECT ?,command_id,seq,stream,data FROM chunks WHERE command_id=?`, result.ReconciliationID, outcome.id); err != nil {
				return result, err
			}
			updated, err := tx.Exec(`UPDATE commands SET state=?,result=?,pid=0 WHERE id=? AND state='unknown'`, outcome.serverResult.State, model.JSON(outcome.serverResult), outcome.id)
			if err != nil {
				return result, err
			}
			changed, err := updated.RowsAffected()
			if err != nil {
				return result, err
			}
			if changed != 1 {
				return result, errors.New("local evidence changed during reconciliation")
			}
			result.AdoptedServerResults++
		}
		rows, err := tx.Query(`SELECT seq,stream,data FROM chunks WHERE command_id=? ORDER BY seq`, outcome.id)
		if err != nil {
			return result, err
		}
		var chunks []model.Chunk
		for rows.Next() {
			chunk := model.Chunk{CommandID: outcome.id}
			if err = rows.Scan(&chunk.Seq, &chunk.Stream, &chunk.Data); err != nil {
				rows.Close()
				return result, err
			}
			chunks = append(chunks, chunk)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return result, err
		}
		for index := range chunks {
			if err = addPending(tx, "command.output", model.CommandEvent{CommandID: outcome.id, Chunk: &chunks[index]}); err != nil {
				return result, err
			}
			result.ReplayedEvents++
		}
		var state string
		var data []byte
		if err = tx.QueryRow(`SELECT state,result FROM commands WHERE id=?`, outcome.id).Scan(&state, &data); err != nil {
			return result, err
		}
		var event model.CommandEvent
		if len(data) > 0 {
			if err = json.Unmarshal(data, &event); err != nil {
				return result, err
			}
		}
		event.CommandID = outcome.id
		event.State = state
		if err = addPending(tx, "command.finished", event); err != nil {
			return result, err
		}
		result.ReplayedEvents++
	}
	if _, err = tx.Exec(`INSERT OR REPLACE INTO metadata(key,value) VALUES('server_epoch',?)`, epoch); err != nil {
		return result, err
	}
	return result, tx.Commit()
}
