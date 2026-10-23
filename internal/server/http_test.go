package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/broker"
	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
	"github.com/coder/websocket"
)

func testEnvelope(t *testing.T, c *websocket.Conn, send *model.Envelope) model.Envelope {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if send != nil {
		if err := c.Write(ctx, websocket.MessageText, model.JSON(send)); err != nil {
			t.Fatal(err)
		}
		return model.Envelope{}
	}
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got model.Envelope
	if err = json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestDisconnectedWaitReleasesExecutorSlotPromptly(t *testing.T) {
	ctx := context.Background()
	s, err := storage.Open(ctx, filepath.Join(t.TempDir(), "broker.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	b := broker.New(s, 10)
	if err = b.CreateWorkspace(ctx, model.Workspace{ID: "workspace", ExecutorID: "executor"}); err != nil {
		t.Fatal(err)
	}
	_, token, err := b.CreateDevice(ctx, model.Principal{Kind: "executor", ExecutorID: "executor"})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(NewHandler(b))
	t.Cleanup(httpServer.Close)
	t.Cleanup(b.Drain)
	dial := func() (*websocket.Conn, model.Envelope) {
		t.Helper()
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/v1/client", &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{"Bearer " + token}}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.CloseNow() })
		testEnvelope(t, conn, &model.Envelope{Version: 1, Type: "hello", Payload: model.JSON(model.Hello{ExecutorID: "executor", StreamID: "journal", Workspaces: []string{"workspace"}})})
		return conn, testEnvelope(t, conn, nil)
	}
	conn, first := dial()
	if first.Type != "welcome" {
		t.Fatalf("first connection rejected: %+v", first)
	}
	var welcome model.Welcome
	if err = json.Unmarshal(first.Payload, &welcome); err != nil {
		t.Fatal(err)
	}
	rpc := func(id, method string, args any) {
		testEnvelope(t, conn, &model.Envelope{Version: 1, Type: "rpc", RequestID: id, ServerEpoch: welcome.ServerEpoch, Generation: welcome.Generation, Payload: model.JSON(model.RPC{Method: method, Args: model.JSON(args)})})
	}
	rpc("submit", "tasks_submit", map[string]any{"workspace_id": "workspace", "prompt": "test wait disconnect", "idempotency_key": "submit-1"})
	submission := testEnvelope(t, conn, nil)
	if submission.Error != nil {
		t.Fatal(submission.Error)
	}
	var task model.Task
	if err = json.Unmarshal(submission.Payload, &task); err != nil {
		t.Fatal(err)
	}
	rpc("wait", "tasks_wait", map[string]any{"task_id": task.ID, "wait_ms": 60000})
	// The following response proves the reader dispatched the wait request,
	// without relying on scheduling sleeps or exposing test-only service hooks.
	rpc("barrier", "tasks_get", map[string]any{"task_id": task.ID})
	barrier := testEnvelope(t, conn, nil)
	if barrier.RequestID != "barrier" {
		t.Fatalf("unexpected response: %+v", barrier)
	}
	_ = conn.CloseNow()
	deadline := time.Now().Add(2 * time.Second)
	for {
		retry, reply := dial()
		_ = retry.CloseNow()
		if reply.Type == "welcome" {
			return
		}
		if reply.Error == nil || reply.Error.Code != "duplicate_session" {
			t.Fatalf("unexpected reconnect response: %+v", reply)
		}
		if !time.Now().Before(deadline) {
			t.Fatal("disconnected tasks_wait retained executor slot for more than 2 seconds")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func unusedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	return addr
}

func TestServeListenerFailureClosesOtherListenerAndReleasesStore(t *testing.T) {
	for _, failing := range []string{"public", "admin"} {
		t.Run(failing, func(t *testing.T) {
			occupied, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer occupied.Close()
			other := unusedAddress(t)
			cfg := Config{Listen: other, AdminListen: occupied.Addr().String(), Database: filepath.Join(t.TempDir(), "broker.db"), AdminTokenEnv: "BRIDGE_TEST_ADMIN_TOKEN", MaxQueuedTasks: 10}
			if failing == "public" {
				cfg.Listen, cfg.AdminListen = cfg.AdminListen, cfg.Listen
			}
			t.Setenv(cfg.AdminTokenEnv, strings.Repeat("a", 48))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err = Serve(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
				t.Fatal("Serve succeeded with occupied listener")
			}
			// ListenAndServe may bind after its sibling has failed; Serve must
			// finish both serving loops before another owner reuses the address.
			listener, err := net.Listen("tcp", other)
			if err != nil {
				t.Fatalf("sibling listener survived failed Serve: %v", err)
			}
			defer listener.Close()
			store, err := storage.Open(context.Background(), cfg.Database)
			if err != nil {
				t.Fatalf("database lock not released: %v", err)
			}
			if err = store.Integrity(context.Background()); err != nil {
				t.Fatal(err)
			}
			_ = store.Close()
		})
	}
}
