package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Zxilly/task-bridge/internal/broker"
	"github.com/Zxilly/task-bridge/internal/mcpagent"
	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func bearer(r *http.Request) string {
	v := r.Header.Get("Authorization")
	if !strings.HasPrefix(v, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(v, "Bearer ")
}
func NewHandler(b *broker.Broker) http.Handler {
	mux := http.NewServeMux()
	auth := func(r *http.Request) (model.Principal, error) { return b.Authenticate(r.Context(), bearer(r)) }
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if !b.Ready() || b.Store.DB.PingContext(ctx) != nil {
			http.Error(w, "not ready", 503)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.Handle("GET /mcp/agent", mcpagent.NewHandler(b, auth))
	mux.HandleFunc("GET /v1/client", func(w http.ResponseWriter, r *http.Request) {
		p, e := auth(r)
		if e != nil {
			http.Error(w, "unauthorized", 401)
			return
		}
		if p.Kind != "executor" {
			http.Error(w, "forbidden", 403)
			return
		}
		serveClient(w, r, b, p)
	})
	return mux
}
func NewAdminHandler(b *broker.Broker, token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/rpc", func(w http.ResponseWriter, r *http.Request) {
		var q model.RPC
		r.Body = http.MaxBytesReader(w, r.Body, model.MaxMessageBytes)
		d := json.NewDecoder(r.Body)
		d.DisallowUnknownFields()
		if e := d.Decode(&q); e != nil {
			http.Error(w, "invalid administrative request", 400)
			return
		}
		out, e := b.Admin(r.Context(), q.Method, q.Args)
		w.Header().Set("Content-Type", "application/json")
		if e != nil {
			w.WriteHeader(400)
			_ = json.NewEncoder(w).Encode(model.ErrorResult{Error: model.AsError(e)})
			return
		}
		_ = json.NewEncoder(w).Encode(model.AdminResponse[any]{Result: out})
	})
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "bridge_agents_connected", Help: "Connected and initialized Claude slots."}, func() float64 { a, _ := b.ConnectionCounts(); return float64(a) }))
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "bridge_executors_connected", Help: "Connected executor clients."}, func() float64 { _, c := b.ConnectionCounts(); return float64(c) }))
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "bridge_storage_allocated_bytes", Help: "SQLite database and WAL allocated bytes."}, func() float64 { return float64(b.Store.Usage()) }))
	for _, entry := range []struct{ name, query string }{{"bridge_commands_running", "SELECT COUNT(*) FROM commands WHERE state IN ('running','accepted','dispatched')"}, {"bridge_commands_unknown", "SELECT COUNT(*) FROM commands WHERE state='unknown'"}, {"bridge_outbox_pending", "SELECT COUNT(*) FROM outbox WHERE acked=0"}} {
		entry := entry
		registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: entry.name, Help: entry.name + " records."}, func() float64 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var n float64
			_ = b.Store.DB.QueryRowContext(ctx, entry.query).Scan(&n)
			return n
		}))
	}
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "bridge_ready", Help: "Whether broker accepts work."}, func() float64 {
		if b.Ready() {
			return 1
		}
		return 0
	}))
	for _, state := range []string{"queued", "offered", "running", "interrupted", "cancel_requested"} {
		state := state
		registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "bridge_tasks_" + state, Help: "Tasks in " + state + " state."}, func() float64 {
			var n float64
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = b.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM tasks WHERE state=?", state).Scan(&n)
			return n
		}))
	}
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(bearer(r)), []byte(token)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		mux.ServeHTTP(w, r)
	})
}
func serveClient(w http.ResponseWriter, r *http.Request, b *broker.Broker, p model.Principal) {
	conn, e := websocket.Accept(w, r, nil)
	if e != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(model.MaxMessageBytes)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
	_, data, e := conn.Read(readCtx)
	readCancel()
	if e != nil {
		return
	}
	var first model.Envelope
	if json.Unmarshal(data, &first) != nil || first.Version != 1 || first.Type != "hello" {
		_ = conn.Close(websocket.StatusPolicyViolation, "hello required")
		return
	}
	var hello model.Hello
	if json.Unmarshal(first.Payload, &hello) != nil || hello.ExecutorID != p.ExecutorID || !boundedID(hello.StreamID) {
		_ = conn.Close(websocket.StatusPolicyViolation, "invalid hello identity")
		return
	}
	e = b.Store.View(ctx, func(tx *storage.Tx) error {
		if len(hello.Workspaces) == 0 || len(hello.Workspaces) > 100 {
			return errors.New("invalid workspace list")
		}
		for _, id := range hello.Workspaces {
			ws, e := tx.Workspace(id)
			if e != nil {
				return e
			}
			if ws.ExecutorID != p.ExecutorID || (p.WorkspaceID != "" && p.WorkspaceID != id) {
				return errors.New("workspace scope mismatch")
			}
		}
		return nil
	})
	if e != nil {
		_ = conn.Close(websocket.StatusPolicyViolation, "workspace scope mismatch")
		return
	}
	queue := make(chan []byte, 128)
	var buffered atomic.Int64
	ready := make(chan struct{})
	var closeOnce sync.Once
	closeConn := func() { closeOnce.Do(func() { cancel(); conn.CloseNow() }) }
	send := func(m model.Envelope) error {
		m.Version = 1
		encoded, e := json.Marshal(m)
		if e != nil {
			return e
		}
		if len(encoded) > model.MaxMessageBytes {
			return errors.New("message too large")
		}
		n := buffered.Add(int64(len(encoded)))
		if n > 4<<20 {
			buffered.Add(-int64(len(encoded)))
			return errors.New("slow consumer")
		}
		select {
		case queue <- encoded:
			return nil
		case <-ctx.Done():
			buffered.Add(-int64(len(encoded)))
			return ctx.Err()
		default:
			buffered.Add(-int64(len(encoded)))
			return errors.New("slow consumer")
		}
	}
	peer, e := b.ConnectClient(ctx, p, hello.StreamID, send, closeConn, hello.Workspaces...)
	if e != nil {
		e2 := model.Envelope{Version: 1, Type: "error", Error: model.AsError(e)}
		encoded, _ := json.Marshal(e2)
		_ = conn.Write(ctx, websocket.MessageText, encoded)
		return
	}
	defer b.DisconnectClient(peer)
	welcome := model.Envelope{Version: 1, Type: "welcome", Payload: model.JSON(model.Welcome{ServerEpoch: b.Store.Epoch, StreamID: b.Store.Epoch + "/" + p.ExecutorID, Generation: peer.Generation})}
	encoded, _ := json.Marshal(welcome)
	writeCtx, writeCancel := context.WithTimeout(ctx, 10*time.Second)
	e = conn.Write(writeCtx, websocket.MessageText, encoded)
	writeCancel()
	if e != nil {
		return
	}
	close(ready)
	var workers sync.WaitGroup
	workers.Add(2)
	defer func() { closeConn(); workers.Wait() }()
	go func() {
		defer workers.Done()
		<-ready
		for {
			select {
			case <-ctx.Done():
				return
			case data := <-queue:
				buffered.Add(-int64(len(data)))
				writeCtx, cancelWrite := context.WithTimeout(ctx, 10*time.Second)
				e := conn.Write(writeCtx, websocket.MessageText, data)
				cancelWrite()
				if e != nil {
					closeConn()
					return
				}
			}
		}
	}()
	go func() {
		defer workers.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pingCtx, cancelPing := context.WithTimeout(ctx, 15*time.Second)
				e := conn.Ping(pingCtx)
				cancelPing()
				if e != nil {
					closeConn()
					return
				}
			}
		}
	}()
	inflight := make(chan struct{}, 64)
	var calls sync.WaitGroup
	// A hijacked request context is not cancelled until the handler returns.
	// Cancel in-flight waits before joining them, so an EOF releases the
	// executor slot promptly and a reconnect can take over.
	defer func() { closeConn(); calls.Wait() }()
	for {
		_, data, e = conn.Read(ctx)
		if e != nil {
			return
		}
		var m model.Envelope
		if json.Unmarshal(data, &m) != nil || m.Version != 1 || m.Generation != peer.Generation || m.ServerEpoch != b.Store.Epoch {
			_ = send(model.Envelope{Version: 1, Type: "error", Error: &model.Error{Code: "invalid_message", Message: "invalid envelope or stale generation"}})
			return
		}
		if e = b.Check(ctx, p); e != nil {
			return
		}
		if m.Type == "rpc" {
			if !boundedID(m.RequestID) {
				return
			}
			var q model.RPC
			if json.Unmarshal(m.Payload, &q) != nil {
				return
			}
			select {
			case inflight <- struct{}{}:
			default:
				_ = send(model.Envelope{Type: "response", RequestID: m.RequestID, Error: &model.Error{Code: "too_many_requests", Message: "too many concurrent calls", Retryable: true}})
				continue
			}
			calls.Add(1)
			go func(id string, q model.RPC) {
				defer calls.Done()
				defer func() { <-inflight }()
				callCtx, cancelCall := context.WithTimeout(ctx, 130*time.Second)
				defer cancelCall()
				out, e := b.CallClient(callCtx, peer, q.Method, q.Args)
				reply := model.Envelope{Type: "response", RequestID: id, ServerEpoch: b.Store.Epoch, Generation: peer.Generation}
				if e != nil {
					reply.Error = model.AsError(e)
				} else {
					reply.Payload = model.JSON(out)
				}
				if e = send(reply); e != nil {
					closeConn()
				}
			}(m.RequestID, q)
			continue
		}
		if m.Type == "ack" {
			if e = b.Ack(ctx, peer, m.RequestID); e != nil {
				return
			}
			continue
		}
		e = b.ClientEvent(ctx, peer, m)
		reply := model.Envelope{Type: "ack", RequestID: m.ID, ServerEpoch: b.Store.Epoch, Generation: peer.Generation, Seq: m.Seq}
		if e != nil {
			reply.Error = model.AsError(e)
			slog.Warn("executor event rejected", "executor_id", p.ExecutorID, "type", m.Type, "code", reply.Error.Code)
		}
		if e = send(reply); e != nil {
			return
		}
	}
}
func boundedID(s string) bool { return len(s) > 0 && len(s) <= 200 }
func Serve(ctx context.Context, cfg Config, log *slog.Logger) error {
	token, e := cfg.AdminToken()
	if e != nil {
		return e
	}
	s, e := storage.Open(ctx, cfg.Database)
	if e != nil {
		return e
	}
	defer s.Close()
	s.MaxBytes = cfg.MaxDatabaseBytes
	if s.MaxBytes <= 0 {
		s.MaxBytes = 2 << 30
	}
	if e = s.Integrity(ctx); e != nil {
		return e
	}
	b := broker.New(s, cfg.MaxQueuedTasks)
	if e = b.Recover(ctx); e != nil {
		return e
	}
	if e = b.Maintain(ctx); e != nil {
		return e
	}
	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() { defer close(runDone); b.Run(runCtx) }()
	// The scheduler can still be inside a transaction when cancellation
	// arrives. Join it before the earlier s.Close defer closes the database.
	defer func() { cancel(); <-runDone }()
	pub := &http.Server{Addr: cfg.Listen, Handler: NewHandler(b), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 32 << 10}
	admin := &http.Server{Addr: cfg.AdminListen, Handler: NewAdminHandler(b, token), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	// Close both listeners on every exit, including a failure to bind only
	// one address. Graceful Shutdown below remains the normal exit path.
	defer pub.Close()
	defer admin.Close()
	errs := make(chan error, 2)
	go func() { errs <- pub.ListenAndServe() }()
	go func() { errs <- admin.ListenAndServe() }()
	log.Info("bridge listening", "listen", cfg.Listen, "admin_listen", cfg.AdminListen, "version", model.Version)
	select {
	case <-ctx.Done():
	case e = <-errs:
		if !errors.Is(e, http.ErrServerClosed) {
			cancel()
			b.Drain()
			return fmt.Errorf("http server: %w", e)
		}
	}
	b.Drain()
	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	_ = pub.Shutdown(stopCtx)
	_ = admin.Shutdown(stopCtx)
	return nil
}
