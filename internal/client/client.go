package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Zxilly/task-bridge/internal/executor"
	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/coder/websocket"
	"github.com/google/uuid"
)

var ErrOffline = errors.New("executor is not connected to the broker; retry using the same idempotency key")

type liveConnection struct {
	conn    *websocket.Conn
	welcome model.Welcome
	ctx     context.Context
	sentMu  sync.Mutex
	sent    map[string]time.Time
}
type Client struct {
	cfg        Config
	journal    *Journal
	logger     *slog.Logger
	mu         sync.Mutex
	live       *liveConnection
	changed    chan struct{}
	pending    map[string]chan model.Envelope
	running    map[string]context.CancelFunc
	workspaces map[string]chan struct{}
	wake       chan struct{}
	runners    sync.WaitGroup
	admitted   int
}

func New(cfg Config, logger *slog.Logger) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	journal, err := OpenJournal(cfg.JournalPath)
	if err != nil {
		return nil, err
	}
	if cfg.MaxJournalBytes > 0 {
		journal.maxBytes = cfg.MaxJournalBytes
	}
	if err = journal.Recover(); err != nil {
		journal.Close()
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	c := &Client{cfg: cfg, journal: journal, logger: logger, changed: make(chan struct{}), pending: map[string]chan model.Envelope{}, running: map[string]context.CancelFunc{}, workspaces: map[string]chan struct{}{}, wake: make(chan struct{}, 1)}
	for id := range cfg.Workspaces {
		c.workspaces[id] = make(chan struct{}, 1)
	}
	return c, nil
}

// Run maintains the outbound connection. Disconnects do not cancel a running
// local command; the caller's root context does (stdio EOF or process shutdown).
func (c *Client) Run(ctx context.Context) error {
	defer func() {
		c.mu.Lock()
		for _, cancel := range c.running {
			cancel()
		}
		c.mu.Unlock()
		c.runners.Wait()
	}()
	backoff := time.Second
	for ctx.Err() == nil {
		err := c.connect(ctx)
		if ctx.Err() != nil {
			return nil
		}
		var fatal *fatalError
		if errors.As(err, &fatal) {
			return fatal.error
		}
		c.logger.Warn("broker connection unavailable; retrying", "error", err)
		delay := backoff/2 + time.Duration(rand.Int64N(int64(backoff/2)+1))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
	return nil
}

type fatalError struct{ error }

func (c *Client) Close() error { return c.journal.Close() }
func (c *Client) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Client) connect(root context.Context) error {
	token, err := c.cfg.Token()
	if err != nil {
		return &fatalError{err}
	}
	dialCtx, cancel := context.WithTimeout(root, 15*time.Second)
	conn, response, err := websocket.Dial(dialCtx, c.cfg.ServerURL, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{"Bearer " + token}}})
	cancel()
	if err != nil {
		if response != nil && (response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden) {
			return &fatalError{model.Err("unauthorized", "executor credentials were rejected or revoked; update them before reconnecting")}
		}
		return err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(model.MaxMessageBytes)
	ctx, stop := context.WithCancel(root)
	defer stop()
	ids := make([]string, 0, len(c.cfg.Workspaces))
	for id := range c.cfg.Workspaces {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	hello := model.Envelope{Version: model.ProtocolVersion, Type: "hello", Payload: model.JSON(model.Hello{ExecutorID: c.cfg.ExecutorID, Version: model.Version, Workspaces: ids, StreamID: c.journal.StreamID()})}
	if err = writeEnvelope(ctx, conn, hello); err != nil {
		return err
	}
	handshakeCtx, endHandshake := context.WithTimeout(ctx, 15*time.Second)
	_, data, err := conn.Read(handshakeCtx)
	endHandshake()
	if err != nil {
		return err
	}
	var envelope model.Envelope
	if err = json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	if envelope.Error != nil {
		if envelope.Error.Code == "journal_mismatch" {
			return &fatalError{envelope.Error}
		}
		return envelope.Error
	}
	if envelope.Type != "welcome" {
		return errors.New("broker did not send welcome")
	}
	var welcome model.Welcome
	if err = json.Unmarshal(envelope.Payload, &welcome); err != nil {
		return err
	}
	if welcome.ServerEpoch == "" || welcome.Generation <= 0 {
		return errors.New("broker welcome lacks recovery epoch or connection generation")
	}
	if err = c.journal.SetServerEpoch(welcome.ServerEpoch); err != nil {
		return &fatalError{err}
	}
	// Retention starts only after this server confirms the stored epoch. A
	// restored server must not trigger cleanup based on an earlier epoch's ACKs.
	if err = c.journal.PruneOutputs(time.Now().Add(-72 * time.Hour).UnixMilli()); err != nil {
		c.logger.Warn("journal output cleanup failed", "error", err)
	}
	live := &liveConnection{conn: conn, welcome: welcome, ctx: ctx, sent: make(map[string]time.Time)}
	c.mu.Lock()
	c.live = live
	close(c.changed)
	c.changed = make(chan struct{})
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.live == live {
			c.live = nil
			close(c.changed)
			c.changed = make(chan struct{})
			for id, ch := range c.pending {
				ch <- model.Envelope{Error: &model.Error{Code: "disconnected", Message: ErrOffline.Error(), Retryable: true}}
				delete(c.pending, id)
			}
		}
		c.mu.Unlock()
	}()
	// Reconcile durable local results before processing replayed dispatches.
	if err = c.flush(ctx, live); err != nil {
		return err
	}
	workers := make(chan error, 2)
	go func() { workers <- c.flushLoop(ctx, live) }()
	go func() { workers <- c.pingLoop(ctx, conn) }()
	defer func() { stop(); conn.CloseNow(); <-workers; <-workers }()
	for {
		_, data, err = conn.Read(ctx)
		if err != nil {
			return err
		}
		var msg model.Envelope
		if err = json.Unmarshal(data, &msg); err != nil {
			return err
		}
		if msg.Version != model.ProtocolVersion {
			return errors.New("unsupported broker protocol")
		}
		if msg.ServerEpoch != "" && msg.ServerEpoch != welcome.ServerEpoch {
			return &fatalError{errors.New("server recovery epoch changed during connection")}
		}
		if msg.Generation != 0 && msg.Generation != welcome.Generation {
			return errors.New("stale broker connection generation")
		}
		if err = c.receive(root, live, msg); err != nil {
			return err
		}
	}
}

func writeEnvelope(ctx context.Context, conn *websocket.Conn, msg model.Envelope) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if len(data) > model.MaxMessageBytes {
		return errors.New("outgoing message exceeds limit")
	}
	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, data)
}
func (c *Client) send(ctx context.Context, live *liveConnection, msg model.Envelope) error {
	msg.Version = model.ProtocolVersion
	msg.Generation = live.welcome.Generation
	msg.ServerEpoch = live.welcome.ServerEpoch
	return writeEnvelope(ctx, live.conn, msg)
}
func (c *Client) flush(ctx context.Context, live *liveConnection) error {
	live.sentMu.Lock()
	messages, err := c.journal.Pending(128)
	if err != nil {
		live.sentMu.Unlock()
		return err
	}
	if live.sent == nil {
		live.sent = make(map[string]time.Time)
	}
	now := time.Now()
	toSend := messages[:0]
	for _, msg := range messages {
		last, alreadySent := live.sent[msg.ID]
		if alreadySent && now.Sub(last) < 5*time.Second {
			continue
		}
		// Mark before writing: an immediate ACK may otherwise remove the ID
		// before this sender inserts it, retaining stale in-flight state.
		live.sent[msg.ID] = now
		toSend = append(toSend, msg)
	}
	live.sentMu.Unlock()
	for _, msg := range toSend {
		if err = c.send(ctx, live, msg); err != nil {
			return err
		}
	}
	return nil
}
func (c *Client) flushLoop(ctx context.Context, live *liveConnection) error {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	prune := time.NewTicker(time.Hour)
	defer prune.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.wake:
		case <-ticker.C:
		case <-prune.C:
			if err := c.journal.PruneOutputs(time.Now().Add(-72 * time.Hour).UnixMilli()); err != nil {
				c.logger.Warn("journal output cleanup failed", "error", err)
			}
		}
		if err := c.flush(ctx, live); err != nil {
			live.conn.CloseNow()
			return err
		}
	}
}
func (c *Client) pingLoop(ctx context.Context, conn *websocket.Conn) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := conn.Ping(pingCtx)
		cancel()
		if err != nil {
			conn.CloseNow()
			return err
		}
	}
}

func (c *Client) receive(root context.Context, live *liveConnection, msg model.Envelope) error {
	switch msg.Type {
	case "response":
		c.mu.Lock()
		if ch := c.pending[msg.RequestID]; ch != nil {
			ch <- msg
			delete(c.pending, msg.RequestID)
		}
		c.mu.Unlock()
	case "ack":
		if msg.Error != nil {
			return msg.Error
		}
		if err := c.journal.Ack(msg.RequestID); err != nil {
			return err
		}
		live.sentMu.Lock()
		delete(live.sent, msg.RequestID)
		live.sentMu.Unlock()
		c.signal()
	case "command.dispatch":
		var dispatch model.Dispatch
		if err := json.Unmarshal(msg.Payload, &dispatch); err != nil {
			return err
		}
		if dispatch.Command.ID == "" {
			return errors.New("dispatch lacks command id")
		}
		fresh, _, err := c.journal.Admit(msg.ID, dispatch)
		if err != nil {
			return err
		}
		if fresh {
			c.mu.Lock()
			allowed := c.admitted < 128
			if allowed {
				c.admitted++
			}
			c.mu.Unlock()
			if !allowed {
				if err = c.journal.Record("command.finished", model.CommandEvent{CommandID: dispatch.Command.ID, State: "not_started", FinishedAt: model.Now(), Error: commandError("executor_busy", "the local executor already has 128 admitted commands")}, 0); err != nil {
					return err
				}
			} else {
				defer func() {
					c.runners.Add(1)
					go func() {
						defer c.runners.Done()
						defer func() { c.mu.Lock(); c.admitted--; c.mu.Unlock() }()
						c.execute(root, dispatch)
					}()
				}()
			}
		}
		if err = c.send(live.ctx, live, model.Envelope{Type: "ack", RequestID: msg.ID, Seq: msg.Seq, StreamID: msg.StreamID}); err != nil {
			return err
		}
		c.signal()
	case "command.cancel":
		var event model.CommandEvent
		if err := json.Unmarshal(msg.Payload, &event); err != nil {
			return err
		}
		if event.CommandID == "" {
			return errors.New("cancel lacks command id")
		}
		if err := c.journal.Cancel(msg.ID, event.CommandID); err != nil {
			return err
		}
		c.mu.Lock()
		if cancel := c.running[event.CommandID]; cancel != nil {
			cancel()
		}
		c.mu.Unlock()
		return c.send(live.ctx, live, model.Envelope{Type: "ack", RequestID: msg.ID, Seq: msg.Seq, StreamID: msg.StreamID})
	case "server.draining":
		return errors.New("broker is draining")
	case "error":
		if msg.Error != nil {
			return msg.Error
		}
		return errors.New("broker reported a protocol error")
	case "task.event", "snapshot", "heartbeat": // Durable task state is queried by the MCP tools.
	default:
		return fmt.Errorf("unsupported broker message type %q", msg.Type)
	}
	return nil
}

func (c *Client) RPC(ctx context.Context, method string, args any) (json.RawMessage, error) {
	encoded, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	if len(encoded) > model.MaxToolBytes {
		return nil, errors.New("tool arguments exceed limit")
	}
	readyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var live *liveConnection
	for {
		c.mu.Lock()
		live = c.live
		changed := c.changed
		c.mu.Unlock()
		if live != nil {
			break
		}
		select {
		case <-readyCtx.Done():
			return nil, ErrOffline
		case <-changed:
		}
	}
	id := uuid.NewString()
	ch := make(chan model.Envelope, 1)
	c.mu.Lock()
	if c.live != live {
		c.mu.Unlock()
		return nil, ErrOffline
	}
	if len(c.pending) >= 128 {
		c.mu.Unlock()
		return nil, errors.New("too many pending broker calls")
	}
	c.pending[id] = ch
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pending, id); c.mu.Unlock() }()
	if err = c.send(ctx, live, model.Envelope{Type: "rpc", RequestID: id, Payload: model.JSON(model.RPC{Method: method, Args: encoded})}); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-live.ctx.Done():
		return nil, ErrOffline
	case response := <-ch:
		if response.Error != nil {
			return nil, response.Error
		}
		return response.Payload, nil
	}
}

func (c *Client) execute(root context.Context, dispatch model.Dispatch) {
	id := dispatch.Command.ID
	ctx, cancel := context.WithCancel(root)
	defer cancel()
	c.mu.Lock()
	c.running[id] = cancel
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.running, id); c.mu.Unlock() }()
	finish := func(event model.CommandEvent) {
		if err := c.journal.Record("command.finished", event, 0); err != nil {
			c.logger.Error("cannot persist command result", "command_id", id, "error", err)
		}
		c.signal()
	}
	notStarted := func(code, message string) {
		finish(model.CommandEvent{CommandID: id, State: "not_started", FinishedAt: model.Now(), Error: commandError(code, message)})
	}
	if err := c.journal.Capacity(); err != nil {
		c.logger.Warn("rejecting new command", "command_id", id, "error", err)
		notStarted("journal_capacity", err.Error())
		return
	}
	ws, ok := c.cfg.Workspaces[dispatch.WorkspaceID]
	if !ok {
		notStarted("unknown_workspace", "the dispatched workspace is not configured on this client")
		return
	}
	semaphore := c.workspaces[dispatch.WorkspaceID]
	select {
	case semaphore <- struct{}{}:
		defer func() { <-semaphore }()
	case <-ctx.Done():
		notStarted("cancelled_before_start", "the command was cancelled before acquiring the workspace execution slot")
		return
	}
	wasCancelled, err := c.journal.Cancelled(id)
	if err != nil {
		notStarted("journal_read_failed", "cannot read durable cancellation state: "+err.Error())
		return
	}
	if wasCancelled || ctx.Err() != nil {
		notStarted("cancelled_before_start", "the command was cancelled before local execution")
		return
	}
	if dispatch.Command.ValidUntil > 0 && model.Now() > dispatch.Command.ValidUntil {
		notStarted("dispatch_expired", "the dispatch validity deadline passed before local execution")
		return
	}
	authCtx, authCancel := context.WithTimeout(ctx, 15*time.Second)
	response, err := c.RPC(authCtx, "command_authorize", model.CommandAuthorizeArgs{CommandID: id, AssignmentID: dispatch.Command.AssignmentID})
	authCancel()
	if err != nil {
		notStarted("authorization_failed", "could not obtain current execution authorization: "+err.Error())
		return
	}
	var auth model.CommandAuthorization
	if json.Unmarshal(response, &auth) != nil {
		notStarted("authorization_failed", "the broker returned invalid execution authorization")
		return
	}
	if !auth.Allowed {
		notStarted("authorization_denied", "the broker no longer authorizes this command assignment")
		return
	}
	wasCancelled, err = c.journal.Cancelled(id)
	if err != nil {
		notStarted("journal_read_failed", "cannot read durable cancellation state: "+err.Error())
		return
	}
	if wasCancelled || ctx.Err() != nil {
		notStarted("cancelled_before_start", "the command was cancelled before local execution")
		return
	}
	if err = c.journal.MarkStarting(id); err != nil {
		c.logger.Error("cannot journal command start", "command_id", id, "error", err)
		notStarted("journal_start_failed", "cannot persist the local execution start: "+err.Error())
		return
	}
	env := executor.DefaultEnv()
	for key, value := range ws.Env {
		env = append(env, key+"="+value)
	}
	started := false
	result := executor.Run(ctx, executor.Config{Root: ws.Root, BashPath: ws.BashPath, Env: env, MaxOutputBytes: c.cfg.MaxOutputBytes}, executor.Request{Command: dispatch.Command.Command, Cwd: dispatch.Command.Cwd, Timeout: time.Duration(dispatch.Command.TimeoutMS) * time.Millisecond}, func(pid int) error {
		started = true
		err := c.journal.Record("command.started", model.CommandEvent{CommandID: id, State: "running", StartedAt: model.Now()}, pid)
		c.signal()
		return err
	}, func(chunk executor.Chunk) error {
		err := c.journal.Record("command.output", model.CommandEvent{CommandID: id, Chunk: &model.Chunk{CommandID: id, Seq: chunk.Sequence, Stream: chunk.Stream, Data: chunk.Data}}, 0)
		c.signal()
		return err
	})
	state := result.State
	if state == "failed" {
		if started {
			state = "unknown"
		} else {
			state = "not_started"
		}
	}
	event := model.CommandEvent{CommandID: id, State: state, Signal: result.Signal, Cwd: result.Cwd, FinishedAt: model.Now(), DurationMS: result.DurationMS, Truncated: result.Truncated, DroppedBytes: result.DroppedBytes}
	if result.Error != "" {
		code := "execution_unknown"
		if !started {
			code = "execution_not_started"
		}
		event.Error = commandError(code, result.Error)
	}
	if started {
		exitCode := result.ExitCode
		event.ExitCode = &exitCode
	}
	finish(event)
}

func commandError(code, message string) *model.Error {
	message = strings.ToValidUTF8(message, "\ufffd")
	if len(message) > 2048 {
		message = message[:2045]
		for !utf8.ValidString(message) {
			message = message[:len(message)-1]
		}
		message += "..."
	}
	return &model.Error{Code: code, Message: message}
}
