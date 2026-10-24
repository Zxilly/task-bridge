// Package broker implements task authorization, state transitions, and routing.
package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
	"github.com/google/uuid"
)

type agent struct {
	p         model.Principal
	id        string
	notify    model.ChannelNotifier
	lastOffer int64
}
type ClientPeer struct {
	Principal  model.Principal
	Generation int64
	Stream     string
	Send       func(model.Envelope) error
	Close      func()
	sent       map[string]int64
	Workspaces map[string]bool
}
type Broker struct {
	Store     *storage.Store
	mu        sync.Mutex
	agents    map[string]*agent
	clients   map[string]*ClientPeer
	closers   map[string]func()
	wake      chan struct{}
	changed   chan struct{}
	MaxQueued int
	draining  bool
}

func New(s *storage.Store, maxQueued int) *Broker {
	if maxQueued <= 0 {
		maxQueued = 1000
	}
	return &Broker{Store: s, agents: map[string]*agent{}, clients: map[string]*ClientPeer{}, closers: map[string]func(){}, wake: make(chan struct{}, 1), changed: make(chan struct{}), MaxQueued: maxQueued}
}
func (b *Broker) signal() {
	b.mu.Lock()
	close(b.changed)
	b.changed = make(chan struct{})
	b.mu.Unlock()
	select {
	case b.wake <- struct{}{}:
	default:
	}
}
func (b *Broker) update(ctx context.Context, fn func(*storage.Tx) error) error {
	b.mu.Lock()
	e := b.Store.Update(ctx, fn)
	b.mu.Unlock()
	if e == nil {
		b.signal()
	}
	return normalize(e)
}
func normalize(e error) error {
	if errors.Is(e, sql.ErrNoRows) {
		return model.Err("not_found", "requested record does not exist")
	}
	return e
}
func (b *Broker) Authenticate(ctx context.Context, token string) (model.Principal, error) {
	var p model.Principal
	e := b.Store.View(ctx, func(tx *storage.Tx) error { var e error; p, e = tx.Authenticate(token); return e })
	return p, e
}
func (b *Broker) Check(ctx context.Context, p model.Principal) error {
	return b.Store.View(ctx, func(tx *storage.Tx) error {
		v, e := tx.Principal(p.ID)
		if e != nil || v.Disabled {
			return model.Err("unauthorized", "device was revoked")
		}
		return nil
	})
}

func (b *Broker) AgentConnected(ctx context.Context, p model.Principal, id string, notify model.ChannelNotifier) error {
	if p.Kind != "claude" {
		return model.Err("forbidden", "Claude credentials required")
	}
	b.mu.Lock()
	if e := b.Check(ctx, p); e != nil {
		b.mu.Unlock()
		return e
	}
	if b.draining {
		b.mu.Unlock()
		return model.Err("draining", "server is draining")
	}
	if b.agents[p.AgentID] != nil {
		b.mu.Unlock()
		return model.Err("duplicate_session", "agent slot already has a live connection")
	}
	b.agents[p.AgentID] = &agent{p: p, id: id, notify: notify}
	b.mu.Unlock()
	b.signal()
	return nil
}
func (b *Broker) AgentDisconnected(p model.Principal, id string) {
	b.mu.Lock()
	a := b.agents[p.AgentID]
	if a == nil || a.id != id {
		b.mu.Unlock()
		return
	}
	delete(b.agents, p.AgentID)
	_ = b.Store.Update(context.Background(), func(tx *storage.Tx) error {
		ts, e := tx.ActiveTasks()
		if e != nil {
			return e
		}
		for _, t := range ts {
			if t.ConnectionID == id && !t.Terminal() {
				cs, e := tx.Commands(t.ID)
				if e != nil {
					return e
				}
				if t.State == "offered" && len(cs) == 0 {
					t.State = "queued"
					t.AssignmentID = ""
					t.ConnectionID = ""
				} else if t.State != "cancel_requested" {
					t.State = "interrupted"
				}
				t.LeaseUntil = 0
				if e = tx.FailTaskResume(&t, "agent_disconnected", "the claiming controller disconnected; request another attempt from B", "accepted"); e != nil {
					return e
				}
				if e = tx.SaveTask(t); e != nil {
					return e
				}
				if e = tx.Event(t.ID, "agent.disconnected", model.TaskStatePayload{State: t.State}); e != nil {
					return e
				}
			}
		}
		return nil
	})
	b.mu.Unlock()
	b.signal()
}
func (b *Broker) ConnectClient(ctx context.Context, p model.Principal, stream string, send func(model.Envelope) error, closePeer func(), workspaces ...string) (*ClientPeer, error) {
	if p.Kind != "executor" {
		return nil, model.Err("forbidden", "executor credentials required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.draining {
		return nil, model.Err("draining", "server is draining")
	}
	if b.clients[p.ExecutorID] != nil {
		return nil, model.Err("duplicate_session", "executor already connected")
	}
	var generation int64
	e := b.Store.Update(ctx, func(tx *storage.Tx) error {
		current, e := tx.Principal(p.ID)
		if e != nil || current.Disabled {
			return model.Err("unauthorized", "device was revoked during handshake")
		}
		var bound string
		e = tx.QueryRowContext(ctx, "SELECT stream FROM peer_cursors WHERE peer=?", p.ExecutorID).Scan(&bound)
		if e == nil && bound != stream {
			return model.Err("journal_mismatch", "executor is bound to another durable journal; preserve it or perform audited reset-journal")
		}
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		if errors.Is(e, sql.ErrNoRows) {
			if _, e = tx.ExecContext(ctx, "INSERT INTO peer_cursors(peer,stream,last_ack) VALUES(?,?,0)", p.ExecutorID, stream); e != nil {
				return e
			}
		}
		key := "generation/" + p.ExecutorID
		v, e := tx.Meta(key)
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		_, _ = fmt.Sscan(v, &generation)
		generation++
		return tx.SetMeta(key, fmt.Sprint(generation))
	})
	if e != nil {
		return nil, e
	}
	c := &ClientPeer{Principal: p, Generation: generation, Stream: stream, Send: send, Close: closePeer, sent: map[string]int64{}}
	if len(workspaces) > 0 {
		c.Workspaces = map[string]bool{}
		for _, id := range workspaces {
			c.Workspaces[id] = true
		}
	} else if p.WorkspaceID != "" {
		c.Workspaces = map[string]bool{p.WorkspaceID: true}
	}
	b.clients[p.ExecutorID] = c
	select {
	case b.wake <- struct{}{}:
	default:
	}
	return c, nil
}
func (b *Broker) DisconnectClient(c *ClientPeer) {
	b.mu.Lock()
	if b.clients[c.Principal.ExecutorID] == c {
		delete(b.clients, c.Principal.ExecutorID)
	}
	b.mu.Unlock()
	b.signal()
}
func (b *Broker) liveClient(c *ClientPeer) bool { return b.clients[c.Principal.ExecutorID] == c }
func supportsWorkspace(c *ClientPeer, id string) bool {
	return c != nil && (len(c.Workspaces) == 0 || c.Workspaces[id])
}
func (b *Broker) Recover(ctx context.Context) error {
	return b.update(ctx, func(tx *storage.Tx) error {
		ts, e := tx.ActiveTasks()
		if e != nil {
			return e
		}
		for _, t := range ts {
			if !t.Terminal() && t.State != "queued" {
				if t.State != "cancel_requested" {
					t.State = "interrupted"
				}
				t.LeaseUntil = 0
				if e = tx.FailTaskResume(&t, "broker_restarted", "the broker restarted after this attempt was claimed; request another attempt from B", "accepted"); e != nil {
					return e
				}
				if e = tx.SaveTask(t); e != nil {
					return e
				}
			}
		}
		return nil
	})
}
func (b *Broker) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	maintenance := time.NewTicker(time.Hour)
	defer maintenance.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-maintenance.C:
			_ = b.Maintain(ctx)
		case <-b.wake:
		}
		if ctx.Err() != nil {
			return
		}
		b.tick(ctx)
	}
}
func (b *Broker) Maintain(ctx context.Context) error {
	if e := b.update(ctx, func(tx *storage.Tx) error { return tx.PruneOutput(model.Now() - 7*24*60*60*1000) }); e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, e := b.Store.DB.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return e
}
func (b *Broker) tick(ctx context.Context) {
	type notice struct {
		a            *agent
		notification model.ChannelNotification
	}
	var notices []notice
	var deliveries []struct {
		peer *ClientPeer
		msg  model.Envelope
	}
	b.mu.Lock()
	if b.draining {
		b.mu.Unlock()
		return
	}
	e := b.Store.Update(ctx, func(tx *storage.Tx) error {
		ts, e := tx.ActiveTasks()
		if e != nil {
			return e
		}
		now := model.Now()
		busy := map[string]bool{}
		agentBusy := map[string]bool{}
		for i := range ts {
			t := &ts[i]
			if t.Terminal() {
				continue
			}
			if t.DeadlineAt <= now && t.State != "cancel_requested" {
				if e = b.cancelTask(tx, t, "deadline"); e != nil {
					return e
				}
			}
			if t.State == "offered" && t.LeaseUntil < now {
				cs, e := tx.Commands(t.ID)
				if e != nil {
					return e
				}
				if len(cs) == 0 {
					t.State = "queued"
					t.AssignmentID = ""
					t.ConnectionID = ""
					if e = tx.SaveTask(*t); e != nil {
						return e
					}
				}
			}
			if t.State != "queued" && !t.Terminal() {
				busy[t.WorkspaceID] = true
				agentBusy[t.AgentID] = true
			}
			if t.State == "running" {
				if a := b.agents[t.AgentID]; a != nil && a.id == t.ConnectionID {
					t.LeaseUntil = now + 60000
					if e = tx.SaveTask(*t); e != nil {
						return e
					}
				}
			}
			if t.State == "cancel_requested" {
				if e = b.finalizeCancel(tx, t); e != nil {
					return e
				}
			}
		}
		for i := range ts {
			t := &ts[i]
			if t.State == "queued" && !busy[t.WorkspaceID] && supportsWorkspace(b.clients[t.ExecutorID], t.WorkspaceID) {
				w, e := tx.Workspace(t.WorkspaceID)
				if e != nil {
					return e
				}
				if w.Quarantined {
					continue
				}
				for _, a := range b.agents {
					if a.p.WorkspaceID == t.WorkspaceID && !agentBusy[a.p.AgentID] && (t.AgentID == "" || t.AgentID == a.p.AgentID) {
						t.State = "offered"
						t.AssignmentID = uuid.NewString()
						t.ConnectionID = a.id
						t.AgentID = a.p.AgentID
						t.LeaseEpoch++
						t.LeaseUntil = now + 60000
						a.lastOffer = 0
						if e = tx.SaveTask(*t); e != nil {
							return e
						}
						if e = tx.Event(t.ID, "task.offered", model.AssignmentPayload{AssignmentID: t.AssignmentID}); e != nil {
							return e
						}
						busy[t.WorkspaceID] = true
						agentBusy[a.p.AgentID] = true
						break
					}
				}
			}
			if (t.State == "offered" || t.State == "interrupted") && b.agents[t.AgentID] != nil {
				a := b.agents[t.AgentID]
				if now-a.lastOffer > 15000 {
					a.lastOffer = now
					content := "New task. Call task_accept with the task_id and assignment_id, then use the remote bash tool and task_complete.\n" + t.Prompt
					if t.State == "interrupted" {
						content = "Interrupted task. Await an explicit resume request from the original B submitter.\n" + t.Prompt
					}
					resumeID := ""
					if t.ResumeRequest != nil && t.ResumeRequest.State == "pending" {
						resumeID = t.ResumeRequest.ID
						content = "B requested resuming this task's original saved conversation. The controller must claim resume_request_id using task_resume before starting Claude.\n" + t.Prompt
					}
					notices = append(notices, notice{
						a: a,
						notification: model.ChannelNotification{
							Content: content,
							Meta: model.ChannelMetadata{
								ResumeRequestID: resumeID,
								TaskID:          t.ID,
								AssignmentID:    t.AssignmentID,
								WorkspaceID:     t.WorkspaceID,
							},
						},
					})
				}
			}
		}
		for executor, p := range b.clients {
			msgs, e := tx.Outbox(executor)
			if e != nil {
				return e
			}
			for _, m := range msgs {
				// An unrelated state update or partial ACK must not replay all
				// in-flight messages. A new connection gets a fresh sent set.
				if last, ok := p.sent[m.ID]; ok && now-last < 5000 {
					continue
				}
				m.ServerEpoch = b.Store.Epoch
				m.StreamID = b.Store.Epoch + "/" + executor
				m.Generation = p.Generation
				deliveries = append(deliveries, struct {
					peer *ClientPeer
					msg  model.Envelope
				}{p, m})
			}
		}
		return nil
	})
	if e == nil {
		for _, d := range deliveries {
			if d.peer.sent == nil {
				d.peer.sent = map[string]int64{}
			}
			d.peer.sent[d.msg.ID] = model.Now()
		}
	}
	b.mu.Unlock()
	if e != nil {
		return
	}
	for _, n := range notices {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		e := n.a.notify(ctx, n.notification)
		cancel()
		if e != nil {
			b.AgentDisconnected(n.a.p, n.a.id)
		}
	}
	for _, d := range deliveries {
		if e := d.peer.Send(d.msg); e != nil {
			d.peer.Close()
		}
	}
}
func (b *Broker) Drain() {
	b.mu.Lock()
	b.draining = true
	peers := []*ClientPeer{}
	for _, p := range b.clients {
		peers = append(peers, p)
	}
	closers := []func(){}
	for _, closeAgent := range b.closers {
		closers = append(closers, closeAgent)
	}
	b.mu.Unlock()
	for _, closeAgent := range closers {
		closeAgent()
	}
	for _, p := range peers {
		_ = p.Send(model.Envelope{Version: 1, Type: "server.draining"})
		p.Close()
	}
	b.signal()
}
func (b *Broker) Ready() bool { b.mu.Lock(); defer b.mu.Unlock(); return !b.draining }
func (b *Broker) ConnectionCounts() (int, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.agents), len(b.clients)
}

type Args struct {
	TaskID               string          `json:"task_id,omitempty"`
	AssignmentID         string          `json:"assignment_id,omitempty"`
	PreviousAssignmentID string          `json:"previous_assignment_id,omitempty"`
	ResumeRequestID      string          `json:"resume_request_id,omitempty"`
	ErrorCode            string          `json:"error_code,omitempty"`
	CommandID            string          `json:"command_id,omitempty"`
	OperationKey         string          `json:"operation_key,omitempty"`
	Command              string          `json:"command,omitempty"`
	Cwd                  string          `json:"cwd,omitempty"`
	TimeoutMS            int64           `json:"timeout_ms,omitempty"`
	WaitMS               int64           `json:"wait_ms,omitempty"`
	WorkspaceID          string          `json:"workspace_id,omitempty"`
	Prompt               string          `json:"prompt,omitempty"`
	Model                string          `json:"model,omitempty"`
	Effort               string          `json:"effort,omitempty"`
	IdempotencyKey       string          `json:"idempotency_key,omitempty"`
	DeadlineMS           int64           `json:"deadline_ms,omitempty"`
	Outcome              string          `json:"outcome,omitempty"`
	Summary              string          `json:"summary,omitempty"`
	Result               json.RawMessage `json:"result,omitempty"`
	Message              string          `json:"message,omitempty"`
	Reason               string          `json:"reason,omitempty"`
	Cursor               int64           `json:"cursor,omitempty"`
	Limit                int             `json:"limit,omitempty"`
	AfterSeq             int64           `json:"after_seq,omitempty"`
}

func parse(raw json.RawMessage) (Args, error) {
	var a Args
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if e := d.Decode(&a); e != nil {
		return a, model.Err("invalid_arguments", e.Error())
	}
	if e := d.Decode(&struct{}{}); e != io.EOF {
		return a, model.Err("invalid_arguments", "one JSON value required")
	}
	if a.WaitMS < 0 || a.WaitMS > 120000 || a.TimeoutMS < 0 || a.TimeoutMS > 900000 || a.Cursor < 0 || a.AfterSeq < 0 {
		return a, model.Err("invalid_arguments", "numeric argument outside supported range")
	}
	for _, id := range []string{a.TaskID, a.AssignmentID, a.PreviousAssignmentID, a.ResumeRequestID, a.CommandID, a.WorkspaceID, a.IdempotencyKey, a.OperationKey} {
		if len(id) > 200 {
			return a, model.Err("invalid_arguments", "identifier exceeds 200 bytes")
		}
	}
	if len(a.Reason) > 2048 {
		return a, model.Err("invalid_arguments", "reason exceeds 2048 bytes")
	}
	return a, nil
}
func bounded(s string, max int) bool { return len(s) > 0 && len(s) <= max }
func activePrincipal(tx *storage.Tx, p model.Principal) error {
	current, e := tx.Principal(p.ID)
	if e != nil || current.Disabled {
		return model.Err("unauthorized", "device was revoked")
	}
	return nil
}
func canonicalResult(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("null")
	}
	var v any
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.UseNumber()
	if d.Decode(&v) != nil {
		return raw
	}
	return model.JSON(v)
}
func allowedTask(p model.Principal, t model.Task) bool {
	if p.Kind == "claude" {
		return p.WorkspaceID == t.WorkspaceID
	}
	return p.ExecutorID == t.ExecutorID && (p.WorkspaceID == "" || p.WorkspaceID == t.WorkspaceID)
}
func getTask(tx *storage.Tx, p model.Principal, id string) (model.Task, error) {
	t, e := tx.Task(id)
	if e != nil {
		return t, e
	}
	if !allowedTask(p, t) {
		return t, model.Err("forbidden", "task outside credential scope")
	}
	return t, nil
}
func (b *Broker) authorized(t model.Task, p model.Principal, cid, assignment string) error {
	a := b.agents[p.AgentID]
	if a == nil || a.id != cid || t.AgentID != p.AgentID || t.ConnectionID != cid || t.AssignmentID != assignment || assignment == "" || t.LeaseUntil <= model.Now() {
		return model.Err("stale_assignment", "current connection and active task assignment required")
	}
	return nil
}
func snapshot(tx *storage.Tx, t model.Task) (model.TaskSnapshot, error) {
	// The channel carries the complete prompt. Keep status responses bounded.
	if len(t.Prompt) > 1024 {
		t.Prompt = strings.ToValidUTF8(t.Prompt[:1024], "") + "…"
	}
	cs, e := tx.Commands(t.ID)
	if e != nil {
		return model.TaskSnapshot{}, e
	}
	more := len(cs) > 10
	if more {
		cs = cs[len(cs)-10:]
	}
	for i := range cs {
		if cs[i].Error != nil && len(cs[i].Error.Message) > 256 {
			cs[i].Error.Message = strings.ToValidUTF8(cs[i].Error.Message[:256], "") + "…"
		}
		cs[i].RequestedCwd = ""
		if len(cs[i].Cwd) > 512 {
			cs[i].Cwd = strings.ToValidUTF8(cs[i].Cwd[:512], "") + "…"
		}
		if len(cs[i].Command) > 256 {
			cs[i].Command = cs[i].Command[:256] + "…"
		}
	}
	return model.TaskSnapshot{Task: t, Commands: cs, CommandsTruncated: more}, nil
}
func (b *Broker) CallAgent(ctx context.Context, p model.Principal, cid, method string, raw json.RawMessage) (model.AgentResult, error) {
	if e := b.Check(ctx, p); e != nil {
		return nil, e
	}
	if p.Kind != "claude" {
		return nil, model.Err("forbidden", "Claude identity required")
	}
	a, e := parse(raw)
	if e != nil {
		return nil, e
	}
	var out model.AgentResult
	var dispatched model.Command
	if method == "command_wait" {
		if a.WaitMS == 0 {
			a.WaitMS = 30000
		}
		return b.waitCommand(ctx, p, a)
	}
	e = b.update(ctx, func(tx *storage.Tx) error {
		if e := activePrincipal(tx, p); e != nil {
			return e
		}
		t, e := getTask(tx, p, a.TaskID)
		if e != nil {
			return e
		}
		switch method {
		case "task_get":
			out, e = snapshot(tx, t)
			return e
		case "task_accept":
			if e = b.authorized(t, p, cid, a.AssignmentID); e != nil {
				return e
			}
			if t.State != "offered" && t.State != "running" {
				return model.Err("invalid_state", "task is not offered")
			}
			t.State = "running"
			if e = tx.SaveTask(t); e != nil {
				return e
			}
			out = t
			return tx.Event(t.ID, "task.running", model.AssignmentPayload{AssignmentID: t.AssignmentID})
		case "task_resume":
			if a.ResumeRequestID != "" {
				if a.PreviousAssignmentID != "" {
					return model.Err("invalid_arguments", "provide resume_request_id or previous_assignment_id, not both")
				}
				if e = b.claimTaskResume(tx, &t, p, cid, a.ResumeRequestID); e != nil {
					return e
				}
				out = t
				return nil
			}
			if a.PreviousAssignmentID == "" {
				return model.Err("invalid_arguments", "resume_request_id or previous_assignment_id is required")
			}
			if t.ResumeRequest != nil && t.ResumeRequest.State == "pending" {
				return model.Err("resume_request_required", "claim the pending B resume request using resume_request_id")
			}
			if t.State != "interrupted" || a.PreviousAssignmentID != t.AssignmentID || t.AgentID != p.AgentID {
				return model.Err("invalid_state", "matching interrupted assignment required")
			}
			slot := b.agents[p.AgentID]
			if slot == nil || slot.id != cid {
				return model.Err("stale_assignment", "current agent connection required")
			}
			if e = b.checkResumeReady(tx, t); e != nil {
				return e
			}
			t.AssignmentID = uuid.NewString()
			t.ConnectionID = cid
			t.LeaseEpoch++
			t.LeaseUntil = model.Now() + 60000
			t.State = "running"
			if e = tx.SaveTask(t); e != nil {
				return e
			}
			out = t
			return tx.Event(t.ID, "task.resumed", model.AssignmentPayload{AssignmentID: t.AssignmentID})
		case "task_resume_failed":
			out, e = b.reportTaskResumeFailure(tx, &t, p, cid, a)
			return e
		case "bash":
			if e = b.authorized(t, p, cid, a.AssignmentID); e != nil {
				return e
			}
			if t.State != "running" {
				return model.Err("invalid_state", "task must be running")
			}
			if !bounded(a.OperationKey, 200) || !bounded(a.Command, 32768) || len(a.Cwd) > 4096 {
				return model.Err("invalid_arguments", "operation_key and bounded command required")
			}
			if a.TimeoutMS == 0 {
				a.TimeoutMS = 120000
			}
			h := storage.Hash(string(model.JSON(commandRequestKey{Command: a.Command, Cwd: a.Cwd, TimeoutMS: a.TimeoutMS})))
			c, err := tx.CommandByKey(t.ID, a.OperationKey)
			if err == nil {
				if c.RequestHash != h {
					return model.Err("idempotency_conflict", "operation_key reused with different command")
				}
				dispatched = c
				return nil
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if !supportsWorkspace(b.clients[t.ExecutorID], t.WorkspaceID) {
				return model.Err("executor_unavailable", "executor is offline")
			}
			if e = b.Store.AdmissionIn(tx); e != nil {
				return e
			}
			cs, e := tx.Commands(t.ID)
			if e != nil {
				return e
			}
			for _, c := range cs {
				if !c.Terminal() || c.State == "unknown" {
					return model.Err("command_busy", "resolve current command before starting another")
				}
			}
			now := model.Now()
			timeout := min(a.TimeoutMS, t.DeadlineAt-now)
			if timeout <= 0 {
				return model.Err("deadline_exceeded", "task deadline passed")
			}
			c = model.Command{ID: uuid.NewString(), TaskID: t.ID, AssignmentID: t.AssignmentID, OperationKey: a.OperationKey, RequestHash: h, Command: a.Command, Cwd: a.Cwd, RequestedCwd: a.Cwd, TimeoutMS: timeout, State: "dispatched", CreatedAt: now, ValidUntil: min(t.LeaseUntil, t.DeadlineAt)}
			if e = tx.SaveCommand(c); e != nil {
				return e
			}
			if e = tx.Enqueue(t.ExecutorID, "command.dispatch", model.Dispatch{WorkspaceID: t.WorkspaceID, Command: c}); e != nil {
				return e
			}
			dispatched = c
			return tx.Event(t.ID, "command.dispatched", model.CommandIDPayload{CommandID: c.ID})
		case "task_progress":
			if e = b.authorized(t, p, cid, a.AssignmentID); e != nil {
				return e
			}
			if t.State != "running" || !bounded(a.Message, 2048) {
				return model.Err("invalid_arguments", "running task and progress message up to 2048 bytes required")
			}
			t.Progress = a.Message
			if e = tx.SaveTask(t); e != nil {
				return e
			}
			out = t
			return tx.Event(t.ID, "task.progress", model.TaskProgressPayload{Message: a.Message})
		case "task_complete":
			if t.Terminal() {
				if t.State == a.Outcome && t.Summary == a.Summary && string(canonicalResult(t.Result)) == string(canonicalResult(a.Result)) {
					out = t
					return nil
				}
				return model.Err("result_conflict", "task already has a final result")
			}
			if e = b.authorized(t, p, cid, a.AssignmentID); e != nil {
				return e
			}
			if t.State != "running" || (a.Outcome != "succeeded" && a.Outcome != "failed") || !bounded(a.Summary, 2048) || len(a.Result) > 4096 {
				return model.Err("invalid_arguments", "valid outcome, bounded summary and result required")
			}
			cs, e := tx.Commands(t.ID)
			if e != nil {
				return e
			}
			for _, c := range cs {
				if !c.Terminal() || c.State == "unknown" {
					return model.Err("command_unresolved", "all commands must have known outcomes")
				}
			}
			t.State = a.Outcome
			t.Summary = a.Summary
			t.Result = canonicalResult(a.Result)
			if e = tx.SaveTask(t); e != nil {
				return e
			}
			out = t
			return tx.Event(t.ID, "task.completed", model.TaskCompletedPayload{State: t.State, Summary: t.Summary})
		case "command_get", "command_output", "command_cancel":
			c, e := tx.Command(a.CommandID)
			if e != nil {
				return e
			}
			if c.TaskID != t.ID {
				return model.Err("forbidden", "command does not belong to task")
			}
			if method == "command_cancel" {
				if !c.Terminal() {
					c.CancelReason = a.Reason
					if c.CancelReason == "" {
						c.CancelReason = "requested"
					}
					if e = tx.SaveCommand(c); e != nil {
						return e
					}
					if e = tx.Enqueue(t.ExecutorID, "command.cancel", model.CommandEvent{CommandID: c.ID, State: "cancelled"}); e != nil {
						return e
					}
				}
				out = c
				return nil
			}
			if method == "command_output" {
				out, e = output(tx, c, a.Cursor, a.Limit)
				return e
			}
			out, e = commandResult(tx, c)
			return e
		default:
			return model.Err("unknown_method", "unknown agent tool")
		}
	})
	if e != nil {
		return nil, e
	}
	if method == "bash" {
		a.CommandID = dispatched.ID
		if a.WaitMS == 0 {
			a.WaitMS = 30000
		}
		return b.waitCommand(ctx, p, a)
	}
	return out, nil
}

func (b *Broker) CallClient(ctx context.Context, c *ClientPeer, method string, raw json.RawMessage) (model.ClientResult, error) {
	p := c.Principal
	if e := b.Check(ctx, p); e != nil {
		return nil, e
	}
	if method == "executor_reconcile" {
		return b.reconcileExecutor(ctx, c, raw)
	}
	a, e := parse(raw)
	if e != nil {
		return nil, e
	}
	if method == "tasks_resume" {
		return b.requestTaskResume(ctx, c, a)
	}
	if method == "tasks_wait" {
		if a.WaitMS == 0 {
			a.WaitMS = 25000
		}
		a.WaitMS = min(a.WaitMS, 60000)
		return b.waitTask(ctx, p, a)
	}
	if method == "tasks_submit" || method == "tasks_run" {
		task, e := b.submitTask(ctx, c, a)
		if e != nil {
			return nil, e
		}
		if method == "tasks_submit" {
			return task, nil
		}
		a.TaskID = task.ID
		if a.WaitMS == 0 {
			a.WaitMS = 25000
		}
		a.WaitMS = min(a.WaitMS, 60000)
		return b.waitTask(ctx, p, a)
	}
	var out model.ClientResult
	e = b.update(ctx, func(tx *storage.Tx) error {
		if e := activePrincipal(tx, p); e != nil {
			return e
		}
		if !b.liveClient(c) {
			return model.Err("stale_connection", "executor connection replaced")
		}
		switch method {
		case "tasks_list":
			limit := a.Limit
			if limit == 0 {
				limit = 20
			}
			if limit < 1 || limit > 50 {
				return model.Err("invalid_arguments", "limit must be 1..50")
			}
			workspace := a.WorkspaceID
			if p.WorkspaceID != "" {
				if workspace != "" && workspace != p.WorkspaceID {
					return model.Err("forbidden", "workspace outside credential scope")
				}
				workspace = p.WorkspaceID
			}
			items, cursor, e := tx.TaskPage(p.ExecutorID, workspace, a.Cursor, min(limit, 20))
			if e != nil {
				return e
			}
			for i := range items {
				items[i].Prompt = ""
				items[i].Result = nil
				items[i].Summary = ""
				items[i].Progress = ""
				items[i].CancelReason = ""
			}
			out = model.TaskListResult{Tasks: items, Cursor: cursor}
			return nil
		case "command_authorize":
			cmd, e := tx.Command(a.CommandID)
			if e != nil {
				return e
			}
			t, e := getTask(tx, p, cmd.TaskID)
			if e != nil {
				return e
			}
			allowed := supportsWorkspace(c, t.WorkspaceID) && cmd.AssignmentID == a.AssignmentID && t.AssignmentID == cmd.AssignmentID && t.State == "running" && t.LeaseUntil > model.Now() && cmd.ValidUntil > model.Now() && cmd.CancelReason == "" && !cmd.Terminal() && t.DeadlineAt > model.Now()
			if slot := b.agents[t.AgentID]; slot == nil || slot.id != t.ConnectionID {
				allowed = false
			}
			out = model.CommandAuthorization{Allowed: allowed}
			return nil
		case "tasks_get", "tasks_cancel", "tasks_events":
			t, e := getTask(tx, p, a.TaskID)
			if e != nil {
				return e
			}
			if method == "tasks_events" {
				limit := a.Limit
				if limit == 0 {
					limit = 10
				}
				if limit < 1 || limit > 10 {
					return model.Err("invalid_arguments", "event limit must be 1..10")
				}
				out, e = tx.Events(t.ID, a.AfterSeq, limit)
				return e
			}
			if method == "tasks_cancel" {
				if !t.Terminal() {
					if e = b.cancelTask(tx, &t, a.Reason); e != nil {
						return e
					}
				}
				out = t
				return nil
			}
			out, e = snapshot(tx, t)
			return e
		default:
			return model.Err("unknown_method", "unknown executor request")
		}
	})
	return out, e
}

func (b *Broker) submitTask(ctx context.Context, c *ClientPeer, a Args) (model.Task, error) {
	p := c.Principal
	var task model.Task
	err := b.update(ctx, func(tx *storage.Tx) error {
		if e := activePrincipal(tx, p); e != nil {
			return e
		}
		if !b.liveClient(c) {
			return model.Err("stale_connection", "executor connection replaced")
		}
		if b.draining {
			return model.Err("draining", "server is draining")
		}
		if !bounded(a.Prompt, 8192) || !bounded(a.IdempotencyKey, 200) {
			return model.Err("invalid_arguments", "bounded prompt and idempotency_key required")
		}
		if e := model.ValidateTaskOptions(a.Model, a.Effort); e != nil {
			return e
		}
		w, e := tx.Workspace(a.WorkspaceID)
		if e != nil {
			return e
		}
		if w.ExecutorID != p.ExecutorID || (p.WorkspaceID != "" && p.WorkspaceID != w.ID) {
			return model.Err("forbidden", "workspace outside executor scope")
		}
		if w.Quarantined {
			return model.Err("workspace_quarantined", w.QuarantineReason)
		}
		if a.DeadlineMS == 0 {
			a.DeadlineMS = 1800000
		}
		if a.DeadlineMS < 1000 || a.DeadlineMS > 86400000 {
			return model.Err("invalid_arguments", "deadline_ms must be between 1000 and 86400000")
		}
		h := storage.Hash(string(model.JSON(taskRequestKey{WorkspaceID: a.WorkspaceID, Prompt: a.Prompt, DeadlineMS: a.DeadlineMS, Model: a.Model, Effort: a.Effort})))
		t, e := tx.TaskByKey(p.ID, a.IdempotencyKey)
		if e == nil {
			if t.RequestHash != h {
				return model.Err("idempotency_conflict", "submission key reused with different content")
			}
			task = t
			return nil
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		// Existing matching submissions retain their original prompt contract.
		// Enforce English only when this request would create a new task.
		if e = model.ValidateEnglishTaskPrompt(a.Prompt); e != nil {
			return e
		}
		if e = b.Store.AdmissionIn(tx); e != nil {
			return e
		}
		var n int
		if e = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM tasks WHERE state='queued'").Scan(&n); e != nil {
			return e
		}
		if n >= b.MaxQueued {
			return model.Err("queue_full", "task queue is full")
		}
		t = model.Task{ID: uuid.NewString(), RequesterID: p.ID, WorkspaceID: w.ID, ExecutorID: w.ExecutorID, Prompt: a.Prompt, Model: a.Model, Effort: a.Effort, State: "queued", IdempotencyKey: a.IdempotencyKey, RequestHash: h, DeadlineAt: model.Now() + a.DeadlineMS, CreatedAt: model.Now()}
		if e = tx.SaveTask(t); e != nil {
			return e
		}
		task = t
		return tx.Event(t.ID, "task.submitted", model.TaskStatePayload{State: "queued"})
	})
	return task, err
}

func (b *Broker) cancelTask(tx *storage.Tx, t *model.Task, reason string) error {
	if err := tx.FailTaskResume(t, "task_cancelled", "the task was cancelled or its original deadline elapsed", "pending", "accepted"); err != nil {
		return err
	}
	t.CancelReason = reason
	if t.CancelReason == "" {
		t.CancelReason = "requested"
	}
	t.State = "cancel_requested"
	t.LeaseUntil = 0
	cs, e := tx.Commands(t.ID)
	if e != nil {
		return e
	}
	for _, c := range cs {
		if !c.Terminal() {
			c.CancelReason = t.CancelReason
			if e = tx.SaveCommand(c); e != nil {
				return e
			}
			if e = tx.Enqueue(t.ExecutorID, "command.cancel", model.CommandEvent{CommandID: c.ID, State: "cancelled"}); e != nil {
				return e
			}
		}
	}
	if e = b.finalizeCancel(tx, t); e != nil {
		return e
	}
	if e = tx.SaveTask(*t); e != nil {
		return e
	}
	return tx.Event(t.ID, "task.cancel_requested", model.TaskCancelRequestedPayload{Reason: t.CancelReason, State: t.State})
}
func (b *Broker) finalizeCancel(tx *storage.Tx, t *model.Task) error {
	cs, e := tx.Commands(t.ID)
	if e != nil {
		return e
	}
	for _, c := range cs {
		if !c.Terminal() || c.State == "unknown" {
			return nil
		}
	}
	t.State = "cancelled"
	if t.CancelReason == "deadline" {
		t.State = "timed_out"
	}
	return tx.SaveTask(*t)
}

func (b *Broker) waitTask(ctx context.Context, p model.Principal, a Args) (model.TaskWaitResult, error) {
	deadline := time.Now().Add(time.Duration(a.WaitMS) * time.Millisecond)
	for {
		var t model.Task
		var result model.TaskSnapshot
		e := b.Store.View(ctx, func(tx *storage.Tx) error {
			var e error
			t, e = getTask(tx, p, a.TaskID)
			if e != nil {
				return e
			}
			result, e = snapshot(tx, t)
			return e
		})
		if e != nil {
			return model.TaskWaitResult{}, normalize(e)
		}
		if t.Terminal() || !time.Now().Before(deadline) {
			return model.TaskWaitResult{Pending: !t.Terminal(), TaskID: t.ID, Snapshot: result}, nil
		}
		timer := time.NewTimer(min(time.Until(deadline), 100*time.Millisecond))
		select {
		case <-ctx.Done():
			timer.Stop()
			return model.TaskWaitResult{}, ctx.Err()
		case <-timer.C:
		}
	}
}
func (b *Broker) waitCommand(ctx context.Context, p model.Principal, a Args) (model.CommandResult, error) {
	deadline := time.Now().Add(time.Duration(a.WaitMS) * time.Millisecond)
	for {
		var c model.Command
		var out model.CommandResult
		e := b.Store.View(ctx, func(tx *storage.Tx) error {
			t, e := getTask(tx, p, a.TaskID)
			if e != nil {
				return e
			}
			c, e = tx.Command(a.CommandID)
			if e != nil {
				return e
			}
			if c.TaskID != t.ID {
				return model.Err("forbidden", "command does not belong to task")
			}
			out, e = commandResult(tx, c)
			return e
		})
		if e != nil {
			return model.CommandResult{}, normalize(e)
		}
		if c.Terminal() || !time.Now().Before(deadline) {
			return out, nil
		}
		timer := time.NewTimer(min(time.Until(deadline), 100*time.Millisecond))
		select {
		case <-ctx.Done():
			timer.Stop()
			return model.CommandResult{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func commandResult(tx *storage.Tx, c model.Command) (model.CommandResult, error) {
	o, e := output(tx, c, 0, model.MaxOutputPage)
	if e != nil {
		return model.CommandResult{}, e
	}
	c.RequestedCwd = ""
	if len(c.Cwd) > 1024 {
		c.Cwd = strings.ToValidUTF8(c.Cwd[:1024], "") + "…"
	}
	if len(c.Command) > 256 {
		c.Command = strings.ToValidUTF8(c.Command[:256], "") + "…"
	}
	return model.CommandResult{OutputResult: o, Command: c, Pending: !c.Terminal()}, nil
}

// Output uses a byte cursor. Bounded raw base64 fragments preserve arbitrary
// bytes, including a UTF-8 sequence split at a page boundary. Display strings
// are returned only when their combined bytes form complete UTF-8.
func output(tx *storage.Tx, c model.Command, cursor int64, limit int) (model.OutputResult, error) {
	if limit == 0 {
		limit = model.MaxOutputPage
	}
	if limit < 1 || limit > model.MaxOutputPage {
		return model.OutputResult{}, model.Err("invalid_arguments", "output limit must be 1..16384")
	}
	// Reserve room for base64, display text, tool metadata and JSON wrapping.
	limit = min(limit, 8<<10)
	chunks, offsets, e := tx.BytePage(c.ID, cursor)
	if e != nil {
		return model.OutputResult{}, e
	}
	stdout, stderr := []byte{}, []byte{}
	raw := []model.OutputChunk{}
	remaining := limit
	var next int64 = cursor
	for i, ch := range chunks {
		offset := offsets[i]
		end := offset + int64(len(ch.Data))
		if end <= cursor {
			continue
		}
		start := int64(0)
		if cursor > offset {
			start = cursor - offset
		}
		n := min(int64(remaining), int64(len(ch.Data))-start)
		if n <= 0 {
			break
		}
		part := ch.Data[start : start+n]
		raw = append(raw, model.OutputChunk{Seq: ch.Seq, Stream: ch.Stream, Offset: offset + start, Data: part})
		if ch.Stream == "stdout" {
			stdout = append(stdout, part...)
		} else {
			stderr = append(stderr, part...)
		}
		next = offset + start + n
		remaining -= int(n)
		if remaining == 0 {
			break
		}
	}
	stdoutText, stdoutOmitted := displayOutput(stdout)
	stderrText, stderrOmitted := displayOutput(stderr)
	return model.OutputResult{CommandID: c.ID, Stdout: stdoutText, Stderr: stderrText, Chunks: raw, TextOmitted: stdoutOmitted || stderrOmitted, Cursor: next, HasMore: next < c.OutputBytes, Truncated: c.Truncated, DroppedBytes: c.DroppedBytes}, nil
}

func displayOutput(data []byte) (string, bool) {
	if utf8.Valid(data) && len(model.JSON(string(data))) <= 8192 {
		return string(data), false
	}
	return "", true
}
