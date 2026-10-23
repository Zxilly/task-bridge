package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
	"github.com/google/uuid"
)

func (b *Broker) Ack(ctx context.Context, c *ClientPeer, id string) error {
	return b.update(ctx, func(tx *storage.Tx) error {
		if e := activePrincipal(tx, c.Principal); e != nil {
			return e
		}
		if !b.liveClient(c) {
			return model.Err("stale_connection", "connection no longer active")
		}
		if e := tx.Ack(c.Principal.ExecutorID, id); e != nil {
			return e
		}
		delete(c.sent, id)
		return nil
	})
}
func (b *Broker) ClientEvent(ctx context.Context, c *ClientPeer, m model.Envelope) error {
	if m.ID == "" || len(m.ID) > 200 || m.Version != 1 {
		return model.Err("invalid_message", "version and stable message_id required")
	}
	if m.ServerEpoch != b.Store.Epoch || m.Generation != c.Generation {
		return model.Err("stale_connection", "server epoch or connection generation mismatch")
	}
	if e := b.Check(ctx, c.Principal); e != nil {
		return e
	}
	var ev model.CommandEvent
	if e := json.Unmarshal(m.Payload, &ev); e != nil {
		return model.Err("invalid_message", "invalid command event")
	}
	if ev.Error != nil && (len(ev.Error.Code) > 64 || len(ev.Error.Message) > 2048) {
		return model.Err("invalid_message", "execution diagnostic exceeds limits")
	}
	return b.update(ctx, func(tx *storage.Tx) error {
		if e := activePrincipal(tx, c.Principal); e != nil {
			return e
		}
		if !b.liveClient(c) {
			return model.Err("stale_connection", "connection no longer active")
		}
		cmd, e := tx.Command(ev.CommandID)
		if e != nil {
			return e
		}
		task, e := getTask(tx, c.Principal, cmd.TaskID)
		if e != nil {
			return e
		}
		fresh, e := tx.Receive(c.Principal.ExecutorID, m.ID, storage.Hash(m.Type+string(m.Payload)))
		if e != nil || !fresh {
			return e
		}
		switch m.Type {
		case "command.accepted":
			if !cmd.Terminal() && cmd.State == "dispatched" {
				cmd.State = "accepted"
			}
		case "command.started":
			if !cmd.Terminal() {
				cmd.State = "running"
				cmd.StartedAt = ev.StartedAt
				cmd.Cwd = ev.Cwd
			}
		case "command.output":
			if ev.Chunk == nil || ev.Chunk.CommandID != cmd.ID || ev.Chunk.Seq <= 0 || (ev.Chunk.Stream != "stdout" && ev.Chunk.Stream != "stderr") || len(ev.Chunk.Data) > 32768 {
				return model.Err("invalid_message", "invalid bounded output chunk")
			}
			chunk := *ev.Chunk
			seen, e := tx.ChunkSeen(chunk)
			if e != nil {
				return e
			}
			if seen {
				return nil
			}
			digest := storage.ChunkDigest(chunk)
			var total int64
			if e = tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(json_extract(payload,'$.output_bytes')),0) FROM commands WHERE task=?", task.ID).Scan(&total); e != nil {
				return e
			}
			allowed := min(int64(len(chunk.Data)), max(int64(0), min((10<<20)-cmd.OutputBytes, (50<<20)-total)))
			if b.Store.AdmissionIn(tx) != nil {
				allowed = 0
			}
			if allowed < int64(len(chunk.Data)) {
				cmd.Truncated = true
				cmd.DroppedBytes += int64(len(chunk.Data)) - allowed
				chunk.Data = chunk.Data[:allowed]
			}
			added, e := tx.AddChunkWithDigest(chunk, digest)
			if e != nil {
				return e
			}
			if added {
				cmd.OutputBytes += int64(len(chunk.Data))
			}
		case "command.finished":
			switch ev.State {
			case "exited", "cancelled", "timed_out", "unknown", "not_started":
			default:
				return model.Err("invalid_message", "invalid final command state")
			}
			if ev.State == "exited" && ev.ExitCode == nil {
				return model.Err("invalid_message", "exited commands require an exit code")
			}
			if cmd.Terminal() {
				sameExit := (cmd.ExitCode == nil && ev.ExitCode == nil) || (cmd.ExitCode != nil && ev.ExitCode != nil && *cmd.ExitCode == *ev.ExitCode)
				if cmd.State != ev.State || !sameExit || cmd.Signal != ev.Signal || cmd.FinishedAt != ev.FinishedAt || cmd.DurationMS != ev.DurationMS {
					return model.Err("terminal_conflict", "command already has a different terminal state")
				}
				if ev.Cwd != "" && (cmd.Cwd == "" || cmd.Cwd == cmd.RequestedCwd) {
					cmd.Cwd = ev.Cwd
					if cmd.Error == nil {
						cmd.Error = ev.Error
					}
					return tx.SaveCommand(cmd)
				}
				if cmd.Error == nil && ev.Error != nil {
					cmd.Error = ev.Error
					return tx.SaveCommand(cmd)
				}
				return nil
			}
			cmd.State = ev.State
			cmd.Error = ev.Error
			cmd.ExitCode = ev.ExitCode
			cmd.Signal = ev.Signal
			cmd.FinishedAt = ev.FinishedAt
			cmd.DurationMS = ev.DurationMS
			cmd.Truncated = cmd.Truncated || ev.Truncated
			cmd.DroppedBytes += ev.DroppedBytes
			if ev.Cwd != "" {
				cmd.Cwd = ev.Cwd
			}
			if cmd.State == "unknown" {
				w, e := tx.Workspace(task.WorkspaceID)
				if e != nil {
					return e
				}
				w.Quarantined = true
				w.QuarantineReason = "command " + cmd.ID + " has an unknown outcome"
				if e = tx.SaveWorkspace(w); e != nil {
					return e
				}
				if !task.Terminal() && task.State != "cancel_requested" {
					task.State = "interrupted"
					task.LeaseUntil = 0
					if e = tx.SaveTask(task); e != nil {
						return e
					}
				}
			}
		default:
			return model.Err("invalid_message", "unsupported executor event")
		}
		if e = tx.SaveCommand(cmd); e != nil {
			return e
		}
		if m.Type != "command.output" {
			if e = tx.Event(task.ID, m.Type, map[string]any{"command_id": cmd.ID, "state": cmd.State}); e != nil {
				return e
			}
		}
		if task.State == "cancel_requested" {
			return b.finalizeCancel(tx, &task)
		}
		return nil
	})
}

func (b *Broker) CreateWorkspace(ctx context.Context, w model.Workspace) error {
	if !bounded(w.ID, 200) || !bounded(w.ExecutorID, 200) {
		return model.Err("invalid_arguments", "workspace id and executor id required")
	}
	return b.update(ctx, func(tx *storage.Tx) error {
		old, e := tx.Workspace(w.ID)
		if e == nil && old.ExecutorID != w.ExecutorID {
			return model.Err("workspace_conflict", "workspace already belongs to another executor")
		}
		if e == nil {
			w.Quarantined = old.Quarantined
			w.QuarantineReason = old.QuarantineReason
		}
		return tx.SaveWorkspace(w)
	})
}
func (b *Broker) CreateDevice(ctx context.Context, p model.Principal) (model.Principal, string, error) {
	if p.Kind != "executor" && p.Kind != "claude" {
		return p, "", model.Err("invalid_arguments", "kind must be executor or claude")
	}
	if p.Kind == "executor" && p.ExecutorID == "" {
		return p, "", model.Err("invalid_arguments", "executor_id required")
	}
	if p.Kind == "claude" && (p.AgentID == "" || p.WorkspaceID == "") {
		return p, "", model.Err("invalid_arguments", "agent_id and workspace_id required")
	}
	p.ID = uuid.NewString()
	token := uuid.NewString() + uuid.NewString()
	p.TokenHash = storage.Hash(token)
	e := b.update(ctx, func(tx *storage.Tx) error {
		if p.WorkspaceID != "" {
			w, e := tx.Workspace(p.WorkspaceID)
			if e != nil {
				return e
			}
			if p.Kind == "executor" && w.ExecutorID != p.ExecutorID {
				return model.Err("forbidden", "workspace executor mismatch")
			}
		}
		ps, e := tx.Principals()
		if e != nil {
			return e
		}
		for _, v := range ps {
			if !v.Disabled && v.Kind == p.Kind && ((p.Kind == "claude" && v.AgentID == p.AgentID) || (p.Kind == "executor" && v.ExecutorID == p.ExecutorID)) {
				return model.Err("device_conflict", "active device with this slot already exists; revoke it before rotating")
			}
		}
		return tx.SavePrincipal(p)
	})
	return p, token, e
}
func (b *Broker) Revoke(ctx context.Context, id string) error {
	var p model.Principal
	e := b.update(ctx, func(tx *storage.Tx) error {
		var e error
		p, e = tx.Principal(id)
		if e != nil {
			return e
		}
		p.Disabled = true
		return tx.SavePrincipal(p)
	})
	if e != nil {
		return e
	}
	b.mu.Lock()
	peer := b.clients[p.ExecutorID]
	var closer func()
	if a := b.agents[p.AgentID]; a != nil && a.p.ID == p.ID {
		closer = b.closers[a.id]
	}
	b.mu.Unlock()
	if peer != nil && peer.Principal.ID == p.ID {
		peer.Close()
	}
	if closer != nil {
		closer()
	}
	return nil
}
func (b *Broker) RegisterAgentCloser(id string, close func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if close == nil {
		delete(b.closers, id)
	} else {
		b.closers[id] = close
	}
}
func (b *Broker) Admin(ctx context.Context, method string, raw json.RawMessage) (any, error) {
	if method == "devices.create" {
		var p model.Principal
		if e := json.Unmarshal(raw, &p); e != nil {
			return nil, e
		}
		p, token, e := b.CreateDevice(ctx, p)
		return map[string]any{"device": p, "token": token}, e
	}
	if method == "workspaces.create" {
		var w model.Workspace
		if e := json.Unmarshal(raw, &w); e != nil {
			return nil, e
		}
		return w, b.CreateWorkspace(ctx, w)
	}
	var a struct {
		ID      string `json:"id"`
		Outcome string `json:"outcome"`
		Reason  string `json:"reason"`
		Output  string `json:"output"`
	}
	if len(raw) > 0 {
		if e := json.Unmarshal(raw, &a); e != nil {
			return nil, e
		}
	}
	if method == "devices.revoke" {
		return map[string]bool{"revoked": true}, b.Revoke(ctx, a.ID)
	}
	if method == "backup" {
		return map[string]string{"output": a.Output}, b.Store.Backup(ctx, a.Output)
	}
	var out any
	e := b.update(ctx, func(tx *storage.Tx) error {
		switch method {
		case "devices.list":
			var e error
			out, e = tx.Principals()
			return e
		case "executors.reset_journal":
			if !bounded(a.Reason, 2048) {
				return model.Err("invalid_arguments", "verified-stopped evidence is required")
			}
			if b.clients[a.ID] != nil {
				return model.Err("executor_online", "stop the executor before resetting its journal binding")
			}
			tasks, e := tx.Tasks()
			if e != nil {
				return e
			}
			for _, t := range tasks {
				if t.ExecutorID == a.ID {
					cs, e := tx.Commands(t.ID)
					if e != nil {
						return e
					}
					for _, c := range cs {
						if !c.Terminal() || c.State == "unknown" {
							return model.Err("reconciliation_required", "resolve all outstanding execution before replacing a journal")
						}
					}
				}
			}
			ps, e := tx.Principals()
			if e != nil {
				return e
			}
			for _, p := range ps {
				if p.Kind == "executor" && p.ExecutorID == a.ID {
					p.Disabled = true
					if e = tx.SavePrincipal(p); e != nil {
						return e
					}
				}
			}
			if _, e = tx.ExecContext(ctx, "DELETE FROM peer_cursors WHERE peer=?", a.ID); e != nil {
				return e
			}
			if e = tx.Resolution(a.ID, "journal_binding_reset", a.Reason, "local-admin"); e != nil {
				return e
			}
			out = map[string]bool{"reset": true, "old_executor_tokens_revoked": true}
			return nil
		case "tasks.inspect":
			t, e := tx.Task(a.ID)
			if e != nil {
				return e
			}
			out, e = snapshot(tx, t)
			return e
		case "tasks.abandon":
			if !bounded(a.Reason, 2048) {
				return model.Err("invalid_arguments", "audited reason required")
			}
			t, e := tx.Task(a.ID)
			if e != nil {
				return e
			}
			if t.Terminal() {
				return model.Err("invalid_state", "task already terminal")
			}
			t.State = "abandoned"
			t.Unresolved = true
			t.Summary = a.Reason
			t.LeaseUntil = 0
			if e = tx.SaveTask(t); e != nil {
				return e
			}
			w, e := tx.Workspace(t.WorkspaceID)
			if e != nil {
				return e
			}
			w.Quarantined = true
			w.QuarantineReason = "abandoned task requires confirmation that old execution stopped"
			if e = tx.SaveWorkspace(w); e != nil {
				return e
			}
			cs, e := tx.Commands(t.ID)
			if e != nil {
				return e
			}
			for _, c := range cs {
				if !c.Terminal() {
					if e = tx.Enqueue(t.ExecutorID, "command.cancel", model.CommandEvent{CommandID: c.ID, State: "cancelled"}); e != nil {
						return e
					}
					c.State = "unknown"
					if e = tx.SaveCommand(c); e != nil {
						return e
					}
				}
			}
			out = t
			return tx.Resolution(t.ID, "abandoned", a.Reason, "local-admin")
		case "commands.resolve":
			if !bounded(a.Reason, 2048) || (a.Outcome != "not_started" && a.Outcome != "stopped") {
				return model.Err("invalid_arguments", "outcome not_started or stopped and verification evidence required")
			}
			c, e := tx.Command(a.ID)
			if e != nil {
				return e
			}
			if c.State != "unknown" {
				return model.Err("invalid_state", "only unknown commands can be manually resolved")
			}
			c.State = "not_started"
			if a.Outcome == "stopped" {
				c.State = "cancelled"
			}
			c.ExitCode = nil
			if e = tx.SaveCommand(c); e != nil {
				return e
			}
			out = c
			return tx.Resolution(c.ID, a.Outcome, a.Reason, "local-admin")
		case "workspaces.release":
			if !bounded(a.Reason, 2048) {
				return model.Err("invalid_arguments", "verification evidence required")
			}
			w, e := tx.Workspace(a.ID)
			if e != nil {
				return e
			}
			ts, e := tx.Tasks()
			if e != nil {
				return e
			}
			for _, t := range ts {
				if t.WorkspaceID == w.ID {
					cs, e := tx.Commands(t.ID)
					if e != nil {
						return e
					}
					for _, c := range cs {
						if !c.Terminal() || c.State == "unknown" {
							return model.Err("reconciliation_required", fmt.Sprintf("command %s still unresolved", c.ID))
						}
					}
				}
			}
			w.Quarantined = false
			w.QuarantineReason = ""
			if e = tx.SaveWorkspace(w); e != nil {
				return e
			}
			out = w
			return tx.Resolution(w.ID, "released", a.Reason, "local-admin")
		default:
			return model.Err("unknown_method", "unknown administrative operation")
		}
	})
	return out, e
}
