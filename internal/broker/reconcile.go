package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
)

type reconciliationRecord struct {
	Error        *model.Error `json:"error,omitempty"`
	CommandID    string       `json:"command_id"`
	TaskID       string       `json:"task_id"`
	WorkspaceID  string       `json:"workspace_id"`
	State        string       `json:"state"`
	Command      string       `json:"command"`
	OperationKey string       `json:"operation_key"`
	Cwd          string       `json:"cwd"`
	TimeoutMS    int64        `json:"timeout_ms"`
	ExitCode     *int         `json:"exit_code,omitempty"`
	Signal       string       `json:"signal,omitempty"`
	StartedAt    int64        `json:"started_at,omitempty"`
	FinishedAt   int64        `json:"finished_at,omitempty"`
	DurationMS   int64        `json:"duration_ms,omitempty"`
}

// reconcileExecutor is an explicit, audited metadata operation. It never grants
// execution, creates a command, or clears quarantine. Unknown IDs are retained
// as evidence rather than being interpreted as work that never happened.
func (b *Broker) reconcileExecutor(ctx context.Context, peer *ClientPeer, raw json.RawMessage) (any, error) {
	var q struct {
		Reason  string                 `json:"reason"`
		Records []reconciliationRecord `json:"records"`
	}
	if len(raw) > 512<<10 {
		return nil, model.Err("invalid_arguments", "reconciliation batch exceeds 512 KiB")
	}
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if e := d.Decode(&q); e != nil {
		return nil, model.Err("invalid_arguments", e.Error())
	}
	if !bounded(q.Reason, 2048) || len(q.Records) > 100 {
		return nil, model.Err("invalid_arguments", "reason and up to 100 records required")
	}
	known, orphans := []string{}, []string{}
	serverResults := map[string]model.CommandEvent{}
	ids := map[string]bool{}
	e := b.update(ctx, func(tx *storage.Tx) error {
		if e := activePrincipal(tx, peer.Principal); e != nil {
			return e
		}
		if !b.liveClient(peer) {
			return model.Err("stale_connection", "executor connection replaced")
		}
		if e := tx.Resolution("executor/"+peer.Principal.ExecutorID, "journal_reconciliation", q.Reason, "executor:"+peer.Principal.ID); e != nil {
			return e
		}
		for _, r := range q.Records {
			if !bounded(r.CommandID, 200) || ids[r.CommandID] {
				return model.Err("invalid_arguments", "unique command ids required")
			}
			ids[r.CommandID] = true
			w, e := tx.Workspace(r.WorkspaceID)
			if e != nil {
				return e
			}
			if w.ExecutorID != peer.Principal.ExecutorID || (peer.Principal.WorkspaceID != "" && peer.Principal.WorkspaceID != w.ID) {
				return model.Err("forbidden", "reconciliation workspace outside executor scope")
			}
			if r.State != "exited" && r.State != "cancelled" && r.State != "timed_out" && r.State != "not_started" && r.State != "unknown" {
				return model.Err("invalid_arguments", "journal must be recovered before reconciliation")
			}
			if r.State == "exited" && r.ExitCode == nil {
				return model.Err("invalid_arguments", "exited journal records require an exit code")
			}
			if r.Error != nil && (len(r.Error.Code) > 64 || len(r.Error.Message) > 2048) {
				return model.Err("invalid_arguments", "execution diagnostic exceeds limits")
			}
			c, e := tx.Command(r.CommandID)
			if errors.Is(e, sql.ErrNoRows) {
				orphans = append(orphans, r.CommandID)
				w.Quarantined = true
				w.QuarantineReason = "executor journal contains commands absent from restored snapshot"
				if e = tx.SaveWorkspace(w); e != nil {
					return e
				}
				if e = tx.Resolution(r.CommandID, "orphan_journal_record", q.Reason, "executor:"+peer.Principal.ID); e != nil {
					return e
				}
				continue
			}
			if e != nil {
				return e
			}
			t, e := getTask(tx, peer.Principal, c.TaskID)
			if e != nil {
				return e
			}
			if c.TaskID != r.TaskID || t.WorkspaceID != r.WorkspaceID || c.Command != r.Command || c.OperationKey != r.OperationKey || c.RequestedCwd != r.Cwd || c.TimeoutMS != r.TimeoutMS {
				return model.Err("reconciliation_conflict", "journal command fingerprint differs from restored record")
			}
			if c.Terminal() && c.State != "unknown" {
				if r.State == "unknown" {
					serverResults[c.ID] = model.CommandEvent{CommandID: c.ID, State: c.State, ExitCode: c.ExitCode, Signal: c.Signal, Cwd: c.Cwd, StartedAt: c.StartedAt, FinishedAt: c.FinishedAt, DurationMS: c.DurationMS, Truncated: c.Truncated, DroppedBytes: c.DroppedBytes, Error: c.Error}
				} else {
					sameExit := (c.ExitCode == nil && r.ExitCode == nil) || (c.ExitCode != nil && r.ExitCode != nil && *c.ExitCode == *r.ExitCode)
					if c.State != r.State || !sameExit || c.Signal != r.Signal || c.FinishedAt != r.FinishedAt || c.DurationMS != r.DurationMS {
						return model.Err("terminal_conflict", "journal contradicts a known server result")
					}
				}
			}
			if !c.Terminal() || c.State == "unknown" {
				c.State = r.State
				c.Error = r.Error
				c.ExitCode = r.ExitCode
				c.Signal = r.Signal
				if r.StartedAt > 0 {
					c.StartedAt = r.StartedAt
				}
				c.FinishedAt = r.FinishedAt
				c.DurationMS = r.DurationMS
				if e = tx.SaveCommand(c); e != nil {
					return e
				}
			}
			if !t.Terminal() {
				t.State = "interrupted"
				t.LeaseUntil = 0
				if e = tx.SaveTask(t); e != nil {
					return e
				}
			}
			w.Quarantined = true
			w.QuarantineReason = "explicit journal reconciliation completed; verify outcomes before release"
			if e = tx.SaveWorkspace(w); e != nil {
				return e
			}
			if e = tx.Resolution(c.ID, "journal_reconciled_"+r.State, q.Reason, "executor:"+peer.Principal.ID); e != nil {
				return e
			}
			known = append(known, c.ID)
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	return map[string]any{"known_command_ids": known, "orphan_command_ids": orphans, "server_results": serverResults}, nil
}
