package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/coder/websocket"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func clientTestBash(t *testing.T) string {
	t.Helper()
	if path := os.Getenv("BRIDGE_TEST_BASH"); path != "" {
		return path
	}
	if runtime.GOOS == "windows" {
		path := `C:\Program Files\Git\bin\bash.exe`
		if _, err := os.Stat(path); err == nil {
			return path
		}
		t.Skip("Git Bash unavailable")
	}
	path, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash unavailable")
	}
	return path
}

func TestRejectedCredentialsStopReconnectLoop(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var requests atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				http.Error(w, "credentials rejected", status)
			}))
			defer srv.Close()
			root := t.TempDir()
			token := filepath.Join(root, "token")
			if e := os.WriteFile(token, []byte("test-token"), 0600); e != nil {
				t.Fatal(e)
			}
			c, e := New(Config{ServerURL: "ws" + strings.TrimPrefix(srv.URL, "http"), ExecutorID: "test-executor", JournalPath: filepath.Join(root, "journal.db"), TokenFile: token, Workspaces: map[string]WorkspaceConfig{"local": {Root: root, BashPath: clientTestBash(t)}}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			e = c.Run(ctx)
			if e == nil || model.AsError(e).Code != "unauthorized" {
				t.Fatalf("credential rejection did not stop client: %v", e)
			}
			if requests.Load() != 1 {
				t.Fatalf("rejected credential retried %d times", requests.Load())
			}
		})
	}
}

func TestReconnectReplaysResultWithoutExecutingAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	root := t.TempDir()
	var attempts atomic.Int64
	finished := make(chan model.CommandEvent, 1)
	dispatch := model.Dispatch{WorkspaceID: "local", Command: model.Command{ID: "command-1", TaskID: "task-1", AssignmentID: "assignment-1", Command: "printf run >> executions.txt; sleep 0.5; printf done", TimeoutMS: 5000}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "bad token", 401)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		generation := attempts.Add(1)
		_, _, err = conn.Read(ctx)
		if err != nil {
			return
		}
		send := func(msg model.Envelope) error {
			msg.Version = 1
			msg.Generation = generation
			msg.ServerEpoch = "server-epoch"
			return writeEnvelope(ctx, conn, msg)
		}
		if send(model.Envelope{Type: "welcome", Payload: model.JSON(model.Welcome{ServerEpoch: "server-epoch", StreamID: "server-stream", Generation: generation})}) != nil {
			return
		}
		// Replay the same dispatch on every connection, as a broker with an
		// unacknowledged delivery would. The local journal must prevent re-run.
		if send(model.Envelope{Type: "command.dispatch", ID: "dispatch-1", Seq: 1, StreamID: "server-stream", Payload: model.JSON(dispatch)}) != nil {
			return
		}
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var msg model.Envelope
			if json.Unmarshal(data, &msg) != nil {
				return
			}
			if msg.Type == "rpc" {
				var rpc model.RPC
				if json.Unmarshal(msg.Payload, &rpc) != nil {
					return
				}
				if rpc.Method == "command_authorize" {
					if send(model.Envelope{Type: "response", RequestID: msg.RequestID, Payload: model.JSON(model.CommandAuthorization{Allowed: true})}) != nil {
						return
					}
				}
				continue
			}
			if msg.Type == "command.started" && generation == 1 {
				return
			}
			if strings.HasPrefix(msg.Type, "command.") {
				if send(model.Envelope{Type: "ack", RequestID: msg.ID}) != nil {
					return
				}
				if msg.Type == "command.finished" {
					var event model.CommandEvent
					if json.Unmarshal(msg.Payload, &event) == nil {
						select {
						case finished <- event:
						default:
						}
					}
				}
			}
		}
	}))
	defer server.Close()
	t.Setenv("BRIDGE_TEST_TOKEN", "test-token")
	c, err := New(Config{ServerURL: "ws" + strings.TrimPrefix(server.URL, "http"), ExecutorID: "executor-1", TokenEnv: "BRIDGE_TEST_TOKEN", JournalPath: filepath.Join(t.TempDir(), "journal.db"), Workspaces: map[string]WorkspaceConfig{"local": {Root: root, BashPath: clientTestBash(t)}}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()
	defer func() { cancel(); <-runDone }()
	select {
	case event := <-finished:
		if event.State != "exited" || event.ExitCode == nil || *event.ExitCode != 0 {
			t.Fatalf("result: %+v", event)
		}
	case <-ctx.Done():
		t.Fatal("result did not survive reconnect")
	}
	if attempts.Load() < 2 {
		t.Fatal("test did not reconnect")
	}
	data, err := os.ReadFile(filepath.Join(root, "executions.txt"))
	if err != nil || string(data) != "run" {
		t.Fatalf("command execution count: %q %v", data, err)
	}
}

func TestMCPWaitDoesNotBlockExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	root := t.TempDir()
	var mu sync.Mutex
	waitID := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_, _, err = conn.Read(ctx)
		if err != nil {
			return
		}
		send := func(msg model.Envelope) error {
			msg.Version = 1
			msg.Generation = 1
			msg.ServerEpoch = "epoch"
			return writeEnvelope(ctx, conn, msg)
		}
		if send(model.Envelope{Type: "welcome", Payload: model.JSON(model.Welcome{ServerEpoch: "epoch", Generation: 1, StreamID: "server"})}) != nil {
			return
		}
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var msg model.Envelope
			if json.Unmarshal(data, &msg) != nil {
				return
			}
			if msg.Type == "rpc" {
				var rpc model.RPC
				if json.Unmarshal(msg.Payload, &rpc) != nil {
					return
				}
				switch rpc.Method {
				case "tasks_wait":
					mu.Lock()
					waitID = msg.RequestID
					mu.Unlock()
					d := model.Dispatch{WorkspaceID: "local", Command: model.Command{ID: "command-wait", TaskID: "task-1", AssignmentID: "assignment-1", Command: "printf concurrent > marker.txt", TimeoutMS: 5000}}
					if send(model.Envelope{Type: "command.dispatch", ID: "dispatch-wait", Payload: model.JSON(d)}) != nil {
						return
					}
				case "command_authorize":
					if send(model.Envelope{Type: "response", RequestID: msg.RequestID, Payload: model.JSON(model.CommandAuthorization{Allowed: true})}) != nil {
						return
					}
				}
			}
			if strings.HasPrefix(msg.Type, "command.") {
				if send(model.Envelope{Type: "ack", RequestID: msg.ID}) != nil {
					return
				}
				if msg.Type == "command.finished" {
					mu.Lock()
					id := waitID
					mu.Unlock()
					if send(model.Envelope{Type: "response", RequestID: id, Payload: model.JSON(model.Task{ID: "task-1", State: "succeeded"})}) != nil {
						return
					}
				}
			}
		}
	}))
	defer server.Close()
	t.Setenv("BRIDGE_TEST_TOKEN", "test-token")
	c, err := New(Config{ServerURL: "ws" + strings.TrimPrefix(server.URL, "http"), ExecutorID: "executor-1", TokenEnv: "BRIDGE_TEST_TOKEN", JournalPath: filepath.Join(t.TempDir(), "journal.db"), Workspaces: map[string]WorkspaceConfig{"local": {Root: root, BashPath: clientTestBash(t)}}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()
	defer func() { cancel(); <-runDone }()
	serverTransport, hostTransport := mcp.NewInMemoryTransports()
	serverSession, err := c.MCPServer().Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer serverSession.Close()
	host := mcp.NewClient(&mcp.Implementation{Name: "test-host", Version: "1"}, nil)
	hostSession, err := host.Connect(ctx, hostTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hostSession.Close()
	result, err := hostSession.CallTool(ctx, &mcp.CallToolParams{Name: "tasks_wait", Arguments: WaitArgs{TaskID: "task-1", WaitMS: 10000}})
	if err != nil || result.IsError {
		t.Fatalf("MCP wait: %+v %v", result, err)
	}
	data, err := os.ReadFile(filepath.Join(root, "marker.txt"))
	if err != nil || string(data) != "concurrent" {
		t.Fatalf("Bash could not execute during MCP wait: %q %v", data, err)
	}
}

func TestPartialAcknowledgementDoesNotAmplifyReplayAndReconnectResetsWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	type observed struct {
		id         string
		generation int64
		at         time.Time
	}
	received := make(chan observed, 32)
	firstConnection := make(chan *websocket.Conn, 1)
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		generation := attempts.Add(1)
		if _, _, err = conn.Read(ctx); err != nil {
			return
		}
		send := func(message model.Envelope) error {
			message.Version = 1
			message.Generation = generation
			message.ServerEpoch = "replay-epoch"
			return writeEnvelope(ctx, conn, message)
		}
		if send(model.Envelope{Type: "welcome", Payload: model.JSON(model.Welcome{ServerEpoch: "replay-epoch", Generation: generation, StreamID: "server-stream"})}) != nil {
			return
		}
		if generation == 1 {
			firstConnection <- conn
		}
		first := true
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var message model.Envelope
			if json.Unmarshal(data, &message) != nil {
				return
			}
			if message.Type != "command.finished" {
				t.Errorf("unexpected replay message %q", message.Type)
				return
			}
			select {
			case received <- observed{id: message.ID, generation: generation, at: time.Now()}:
			case <-ctx.Done():
				return
			}
			// Only the first event is ACKed on connection one. Receiving that
			// ACK wakes flushLoop while the remaining two are still pending.
			if generation > 1 || first {
				if send(model.Envelope{Type: "ack", RequestID: message.ID}) != nil {
					return
				}
			}
			first = false
		}
	}))
	defer server.Close()
	t.Setenv("BRIDGE_REPLAY_TEST_TOKEN", "test-token")
	c, err := New(Config{ServerURL: "ws" + strings.TrimPrefix(server.URL, "http"), ExecutorID: "executor-replay", TokenEnv: "BRIDGE_REPLAY_TEST_TOKEN", JournalPath: filepath.Join(t.TempDir(), "journal.db"), Workspaces: map[string]WorkspaceConfig{"local": {Root: t.TempDir(), BashPath: clientTestBash(t)}}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for n := 0; n < 3; n++ {
		if err = c.journal.Record("command.finished", model.CommandEvent{CommandID: fmt.Sprintf("pending-%d", n), State: "unknown", FinishedAt: model.Now()}, 0); err != nil {
			t.Fatal(err)
		}
	}
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()
	defer func() { cancel(); <-runDone }()
	next := func(wait time.Duration) observed {
		t.Helper()
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case message := <-received:
			return message
		case <-timer.C:
			t.Fatal("durable pending events did not replay")
		case <-ctx.Done():
			t.Fatal("replay test context ended")
		}
		return observed{}
	}
	initial := map[string]time.Time{}
	acked := ""
	for n := 0; n < 3; n++ {
		message := next(3 * time.Second)
		if message.generation != 1 {
			t.Fatal("unexpected reconnect during initial flush")
		}
		if _, ok := initial[message.id]; ok {
			t.Fatal("partial ACK caused immediate duplicate before initial batch completed")
		}
		initial[message.id] = message.at
		if n == 0 {
			acked = message.id
		}
	}
	remaining := map[string]bool{}
	var latestRetry time.Time
	for n := 0; n < 2; n++ {
		message := next(8 * time.Second)
		if message.generation != 1 || message.id == acked {
			t.Fatalf("wrong retry: %+v", message)
		}
		firstAt, ok := initial[message.id]
		if !ok || remaining[message.id] {
			t.Fatalf("unexpected retry ID %q", message.id)
		}
		if message.at.Sub(firstAt) < 5*time.Second {
			t.Fatalf("partial ACK amplified replay after only %s", message.at.Sub(firstAt))
		}
		remaining[message.id] = true
		latestRetry = message.at
	}
	// A new connection must replay immediately, even though the last retry on
	// the old connection happened less than five seconds ago.
	(<-firstConnection).CloseNow()
	for n := 0; n < 2; n++ {
		message := next(3 * time.Second)
		if message.generation != 2 || !remaining[message.id] {
			t.Fatalf("reconnect failed to replay retained event: %+v", message)
		}
		if message.at.Sub(latestRetry) >= 5*time.Second {
			t.Fatal("reconnect retained the previous connection's retry window")
		}
		delete(remaining, message.id)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pending, err := c.journal.Pending(10)
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) == 0 {
			c.mu.Lock()
			live := c.live
			c.mu.Unlock()
			if live == nil {
				t.Fatal("connection lost after acknowledgement")
			}
			live.sentMu.Lock()
			sent := len(live.sent)
			live.sentMu.Unlock()
			if sent == 0 {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("ACK did not retire both durable and in-flight replay state")
}

func TestExecutionDiagnosticsPreserveFailureReasonAndNonzeroExit(t *testing.T) {
	c, ctx := clientMCPTestPeer(t, func(rpc model.RPC) model.Envelope {
		var args model.CommandAuthorizeArgs
		if json.Unmarshal(rpc.Args, &args) != nil {
			return model.Envelope{Error: model.AsError(fmt.Errorf("bad arguments"))}
		}
		if args.CommandID == "auth-failure" {
			return model.Envelope{Error: &model.Error{Code: "forbidden", Message: "review current assignment"}}
		}
		return model.Envelope{Payload: model.JSON(model.CommandAuthorization{Allowed: args.CommandID != "auth-denied"})}
	})
	var err error
	c.journal, err = OpenJournal(filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.journal.Close()
	c.cfg.Workspaces = map[string]WorkspaceConfig{"local": {Root: t.TempDir(), BashPath: clientTestBash(t)}}
	c.workspaces = map[string]chan struct{}{"local": make(chan struct{}, 1)}
	c.running = map[string]context.CancelFunc{}
	c.wake = make(chan struct{}, 1)
	for _, test := range []struct {
		id, workspace, cwd, command, state, code string
		expired, cancelled                       bool
	}{
		{id: "workspace", workspace: "missing", command: "true", state: "not_started", code: "unknown_workspace"},
		{id: "expired", workspace: "local", command: "true", state: "not_started", code: "dispatch_expired", expired: true},
		{id: "cancelled", workspace: "local", command: "true", state: "not_started", code: "cancelled_before_start", cancelled: true},
		{id: "auth-denied", workspace: "local", command: "true", state: "not_started", code: "authorization_denied"},
		{id: "auth-failure", workspace: "local", command: "true", state: "not_started", code: "authorization_failed"},
		{id: "cwd", workspace: "local", cwd: "missing", command: "true", state: "not_started", code: "execution_not_started"},
		{id: "exit-seven", workspace: "local", command: "exit 7", state: "exited"},
	} {
		t.Run(test.id, func(t *testing.T) {
			dispatch := model.Dispatch{WorkspaceID: test.workspace, Command: model.Command{ID: test.id, TaskID: "task", AssignmentID: "assignment", Command: test.command, Cwd: test.cwd, TimeoutMS: 2000}}
			if test.expired {
				dispatch.Command.ValidUntil = model.Now() - 1
			}
			if _, _, err = c.journal.Admit("dispatch-"+test.id, dispatch); err != nil {
				t.Fatal(err)
			}
			if test.cancelled {
				if err = c.journal.Cancel("cancel-"+test.id, test.id); err != nil {
					t.Fatal(err)
				}
			}
			c.execute(ctx, dispatch)
			var data []byte
			if err = c.journal.db.QueryRow(`SELECT result FROM commands WHERE id=?`, test.id).Scan(&data); err != nil {
				t.Fatal(err)
			}
			var event model.CommandEvent
			if err = json.Unmarshal(data, &event); err != nil {
				t.Fatal(err)
			}
			if event.State != test.state {
				t.Fatalf("state=%s want %s", event.State, test.state)
			}
			if test.code != "" {
				if event.Error == nil || event.Error.Code != test.code || event.Error.Message == "" {
					t.Fatalf("missing execution diagnostic: %+v", event)
				}
			} else if event.Error != nil || event.ExitCode == nil || *event.ExitCode != 7 {
				t.Fatalf("nonzero exit became a transport error: %+v", event)
			}
		})
	}
	detail := commandError("execution_unknown", strings.Repeat("界", 1000)+string([]byte{255}))
	if len(detail.Message) > 2048 || !utf8.ValidString(detail.Message) {
		t.Fatal("execution diagnostic is not bounded UTF-8")
	}
}
