package mcpagent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type observedCall struct {
	principal          model.Principal
	connectionID, name string
	args               json.RawMessage
}
type fakeService struct {
	connected    chan string
	disconnected chan string
	calls        chan observedCall
	notification *model.ChannelNotification
	connectErr   error
	call         func(context.Context, string, json.RawMessage) (model.AgentResult, error)
	closers      sync.Map
}

func (s *fakeService) RegisterAgentCloser(id string, closePeer func()) {
	if closePeer == nil {
		s.closers.Delete(id)
	} else {
		s.closers.Store(id, closePeer)
	}
}

func newFake() *fakeService {
	return &fakeService{connected: make(chan string, 4), disconnected: make(chan string, 4), calls: make(chan observedCall, 128)}
}
func (s *fakeService) AgentConnected(ctx context.Context, p model.Principal, cid string, notify model.ChannelNotifier) error {
	s.connected <- cid
	if s.connectErr != nil {
		return s.connectErr
	}
	if s.notification != nil {
		return notify(ctx, *s.notification)
	}
	return nil
}
func (s *fakeService) AgentDisconnected(p model.Principal, cid string) { s.disconnected <- cid }
func (s *fakeService) CallAgent(ctx context.Context, p model.Principal, cid, name string, args json.RawMessage) (model.AgentResult, error) {
	s.calls <- observedCall{p, cid, name, args}
	if s.call != nil {
		return s.call(ctx, name, args)
	}
	return model.Task{ID: "test-task", State: "running"}, nil
}

func testServer(t *testing.T, service *fakeService) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(NewHandler(service, func(r *http.Request) (model.Principal, error) {
		if r.Header.Get("Authorization") != "Bearer test-agent-token" {
			return model.Principal{}, errors.New("bad token")
		}
		return model.Principal{ID: "device-a", Kind: "claude", AgentID: "a", WorkspaceID: "workspace-b"}, nil
	}))
	t.Cleanup(server.Close)
	return server
}
func dial(t *testing.T, server *httptest.Server) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &websocket.DialOptions{Subprotocols: []string{"mcp"}, HTTPHeader: http.Header{"Authorization": []string{"Bearer test-agent-token"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}
func sendJSON(t *testing.T, c *websocket.Conn, v any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageText, model.JSON(v)); err != nil {
		t.Fatal(err)
	}
}
func receiveJSON(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	return v
}
func initialize(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	sendJSON(t, c, map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{"protocolVersion": "2026-07-28", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "compatibility-test", "version": "1"}}})
	return receiveJSON(t, c)
}

func TestInitializePinsProtocolAndChannelOnlyAfterInitialized(t *testing.T) {
	s := newFake()
	s.notification = &model.ChannelNotification{
		Content: "Task offered",
		Meta:    model.ChannelMetadata{TaskID: "task-1", AssignmentID: "assignment-1", WorkspaceID: "workspace-b"},
	}
	server := testServer(t, s)
	conn := dial(t, server)
	if conn.Subprotocol() != "mcp" {
		t.Fatalf("subprotocol=%q", conn.Subprotocol())
	}
	response := initialize(t, conn)
	result := response["result"].(map[string]any)
	if result["protocolVersion"] != ProtocolVersion {
		t.Fatalf("protocol=%v", result["protocolVersion"])
	}
	experimental := result["capabilities"].(map[string]any)["experimental"].(map[string]any)
	if _, ok := experimental["claude/channel"]; !ok {
		t.Fatal("channel capability missing")
	}
	select {
	case <-s.connected:
		t.Fatal("agent registered before initialized")
	default:
	}
	sendJSON(t, conn, map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized", "params": map[string]any{}})
	receiveChannelNotification(t, conn, *s.notification)
	var cid string
	select {
	case cid = <-s.connected:
	case <-time.After(5 * time.Second):
		t.Fatal("AgentConnected not called")
	}
	if _, err := uuid.Parse(cid); err != nil {
		t.Fatalf("connection ID not UUID: %q", cid)
	}
	_ = conn.CloseNow()
	select {
	case disconnected := <-s.disconnected:
		if disconnected != cid {
			t.Fatalf("disconnected ID=%q", disconnected)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AgentDisconnected not called")
	}
}

func TestHandshakeRejectsUnauthorizedOriginAndMissingSubprotocol(t *testing.T) {
	s := newFake()
	server := testServer(t, s)
	for _, tc := range []struct {
		name, token, origin string
		protocols           []string
		status              int
	}{
		{"missing authentication", "", "", []string{"mcp"}, http.StatusUnauthorized},
		{"cross origin", "Bearer test-agent-token", "https://untrusted.example", []string{"mcp"}, http.StatusForbidden},
		{"missing subprotocol", "Bearer test-agent-token", "", nil, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, res, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &websocket.DialOptions{Subprotocols: tc.protocols, HTTPHeader: http.Header{"Authorization": []string{tc.token}, "Origin": []string{tc.origin}}})
			if conn != nil {
				_ = conn.CloseNow()
			}
			if err == nil || res == nil || res.StatusCode != tc.status {
				t.Fatalf("status=%v err=%v", res, err)
			}
		})
	}
	select {
	case <-s.connected:
		t.Fatal("rejected peer registered")
	default:
	}
}

func TestRegisteredCloserDisconnectsAndUnregisters(t *testing.T) {
	s := newFake()
	server := testServer(t, s)
	conn := dial(t, server)
	initialize(t, conn)
	sendJSON(t, conn, map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized", "params": map[string]any{}})
	var cid string
	select {
	case cid = <-s.connected:
	case <-time.After(5 * time.Second):
		t.Fatal("not connected")
	}
	closePeer, ok := s.closers.Load(cid)
	if !ok {
		t.Fatal("revocation closer was not registered")
	}
	closePeer.(func())()
	select {
	case disconnected := <-s.disconnected:
		if disconnected != cid {
			t.Fatalf("wrong disconnected ID %s", disconnected)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("revocation did not disconnect")
	}
	if _, ok := s.closers.Load(cid); ok {
		t.Fatal("closer not unregistered")
	}
}

func TestOfficialSDKClientListsValidatesAndRoutesTools(t *testing.T) {
	s := newFake()
	server := testServer(t, s)
	conn := dial(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := mcp.NewClient(&mcp.Implementation{Name: "official-sdk-test", Version: "1"}, nil)
	session, err := client.Connect(ctx, NewTransport(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	list, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Tools) != 10 {
		t.Fatalf("tool count=%d", len(list.Tools))
	}
	want := map[string]bool{"task_accept": true, "task_get": true, "task_resume": true, "bash": true, "command_get": true, "command_wait": true, "command_output": true, "command_cancel": true, "task_progress": true, "task_complete": true}
	for _, tool := range list.Tools {
		if !want[tool.Name] {
			t.Fatalf("unexpected tool %q", tool.Name)
		}
		delete(want, tool.Name)
		schema := tool.InputSchema.(map[string]any)
		if schema["additionalProperties"] != false {
			t.Fatalf("tool %s allows unknown arguments", tool.Name)
		}
	}
	for _, tc := range []struct {
		name  string
		args  map[string]any
		valid bool
	}{
		{"task_get", map[string]any{"task_id": "task-1"}, true},
		{"task_get", map[string]any{"task_id": ""}, false},
		{"task_get", map[string]any{"task_id": strings.Repeat("t", 128)}, true},
		{"task_get", map[string]any{"task_id": strings.Repeat("t", 129)}, false},
		{"bash", map[string]any{"task_id": "task-1", "assignment_id": "assignment-1", "operation_key": "op-1", "command": "pwd", "wait_ms": 0}, true},
		{"bash", map[string]any{"task_id": "task-1", "assignment_id": "assignment-1", "operation_key": "op-1", "command": "pwd", "timeout_ms": 0}, false},
		{"bash", map[string]any{"task_id": "task-1", "assignment_id": "assignment-1", "operation_key": "op-1", "command": "pwd", "wait_ms": 120001}, false},
		{"bash", map[string]any{"task_id": "task-1", "command": "pwd"}, false},
		{"task_get", map[string]any{"task_id": "task-1", "executor_id": "other-device"}, false},
		{"command_output", map[string]any{"task_id": "task-1", "command_id": "command-1", "cursor": 0, "limit": 16384}, true},
		{"command_output", map[string]any{"task_id": "task-1", "command_id": "command-1", "cursor": -1}, false},
		{"command_output", map[string]any{"task_id": "task-1", "command_id": "command-1", "cursor": 1.5}, false},
		{"command_output", map[string]any{"task_id": "task-1", "command_id": "command-1", "cursor": 1<<53 - 1}, true},
		{"command_output", map[string]any{"task_id": "task-1", "command_id": "command-1", "cursor": 1 << 53}, false},
		{"command_output", map[string]any{"task_id": "task-1", "command_id": "command-1", "limit": 0}, false},
		{"command_output", map[string]any{"task_id": "task-1", "command_id": "command-1", "limit": 16385}, false},
		{"task_complete", map[string]any{"task_id": "task-1", "assignment_id": "assignment-1", "outcome": "unknown", "summary": "done"}, false},
		{"task_complete", map[string]any{"task_id": "task-1", "assignment_id": "assignment-1", "outcome": "succeeded", "summary": "done", "result": nil}, true},
		{"task_complete", map[string]any{"task_id": "task-1", "assignment_id": "assignment-1", "outcome": "failed", "summary": "done", "result": []any{true, "text", 3}}, true},
	} {
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tc.name, Arguments: tc.args})
		if err != nil {
			t.Fatal(err)
		}
		if result.IsError == tc.valid {
			t.Fatalf("%s args=%v IsError=%v", tc.name, tc.args, result.IsError)
		}
		if !tc.valid {
			if result.StructuredContent != nil {
				t.Fatal("validation error duplicated in structuredContent")
			}
			var body struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal([]byte(result.Content[0].(*mcp.TextContent).Text), &body); err != nil || body.Error.Code != "invalid_arguments" {
				t.Fatalf("validation error lost domain envelope: %+v err=%v", body, err)
			}
		}
		if tc.valid {
			select {
			case call := <-s.calls:
				if call.name != tc.name || call.principal.ID != "device-a" || call.connectionID == "" {
					t.Fatalf("bad routing: %+v", call)
				}
			case <-ctx.Done():
				t.Fatal("call not routed")
			}
		}
	}
	select {
	case call := <-s.calls:
		t.Fatalf("invalid args reached service: %+v", call)
	default:
	}
	// Schema validation may inspect decoded numbers, but must forward the raw
	// result without losing integers that cannot be represented by float64.
	raw := json.RawMessage(`{"task_id":"task-1","assignment_id":"assignment-1","outcome":"succeeded","summary":"完成","result":{"precise":9007199254740993}}`)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "task_complete", Arguments: raw})
	if err != nil || result.IsError {
		t.Fatalf("raw result rejected: result=%+v err=%v", result, err)
	}
	select {
	case call := <-s.calls:
		if string(call.args) != string(raw) {
			t.Fatalf("raw arguments changed: got %s, want %s", call.args, raw)
		}
	case <-ctx.Done():
		t.Fatal("raw result not routed")
	}
}

func TestToolResultBoundsEscapedJSONAndDomainErrors(t *testing.T) {
	for _, value := range []any{strings.Repeat("x", model.MaxToolBytes), strings.Repeat("\x00", model.MaxToolBytes/4), make(chan int)} {
		result := toolResult(value, nil)
		data, err := json.Marshal(result)
		if err != nil || len(data) > model.MaxToolBytes || !result.IsError {
			t.Fatalf("unbounded/invalid result: bytes=%d err=%v", len(data), err)
		}
	}
	result := toolResult(nil, model.Err("lease_expired", "assignment expired"))
	if !result.IsError || !strings.Contains(result.Content[0].(*mcp.TextContent).Text, "lease_expired") {
		t.Fatalf("domain error lost: %+v", result)
	}
}

func TestMCPCallCancellationOnlyCancelsWaitContext(t *testing.T) {
	s := newFake()
	started := make(chan struct{})
	cancelled := make(chan struct{})
	var once sync.Once
	s.call = func(ctx context.Context, name string, args json.RawMessage) (model.AgentResult, error) {
		once.Do(func() { close(started) })
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	}
	server := testServer(t, s)
	conn := dial(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := mcp.NewClient(&mcp.Implementation{Name: "cancel-test", Version: "1"}, nil)
	session, err := client.Connect(ctx, NewTransport(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	waitCtx, cancelWait := context.WithCancel(ctx)
	finished := make(chan error, 1)
	go func() {
		_, err := session.CallTool(waitCtx, &mcp.CallToolParams{Name: "command_wait", Arguments: map[string]any{"task_id": "t", "command_id": "c"}})
		finished <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("wait not started")
	}
	cancelWait()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("cancelled MCP request succeeded")
		}
	case <-ctx.Done():
		t.Fatal("client cancellation blocked")
	}
	select {
	case <-cancelled:
	case <-ctx.Done():
		t.Fatal("service wait context not cancelled")
	}
	call := <-s.calls
	if call.name != "command_wait" {
		t.Fatalf("unexpected business mutation: %q", call.name)
	}
	select {
	case call := <-s.calls:
		t.Fatalf("cancellation invoked %q", call.name)
	default:
	}
}

func TestOfficialSDKProgressOnlyForTokenedWaitCalls(t *testing.T) {
	s := newFake()
	server := testServer(t, s)
	conn := dial(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	progress := make(chan mcp.ProgressNotificationParams, 16)
	client := mcp.NewClient(&mcp.Implementation{Name: "progress-test", Version: "1"}, &mcp.ClientOptions{ProgressNotificationHandler: func(_ context.Context, req *mcp.ProgressNotificationClientRequest) { progress <- *req.Params }})
	session, err := client.Connect(ctx, NewTransport(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	for _, tc := range []struct {
		name  string
		token any
		want  bool
	}{
		{"bash", "bash-token", true},
		{"command_wait", float64(0), true},
		{"command_wait", nil, false},
		{"command_wait", true, false},
		{"task_get", "read-token", false},
	} {
		args := map[string]any{"task_id": "task-1"}
		if tc.name == "bash" {
			args["assignment_id"] = "assignment-1"
			args["operation_key"] = "op-1"
			args["command"] = "true"
		}
		if tc.name == "command_wait" {
			args["command_id"] = "command-1"
		}
		params := &mcp.CallToolParams{Name: tc.name, Arguments: args}
		if tc.token != nil {
			params.Meta = mcp.Meta{"progressToken": tc.token}
		}
		result, err := session.CallTool(ctx, params)
		if err != nil || result.IsError {
			t.Fatalf("%s call failed: result=%+v err=%v", tc.name, result, err)
		}
		if tc.want {
			for _, want := range []float64{0, 1} {
				select {
				case got := <-progress:
					if got.ProgressToken != tc.token || got.Progress != want || got.Total != 1 {
						t.Fatalf("unexpected progress: %+v", got)
					}
					if want == 1 && !strings.Contains(got.Message, "this tool call") { // Check wording without interpreting command outcome.
						if !strings.Contains(got.Message, "This tool call") {
							t.Fatalf("completion message does not identify wait scope: %q", got.Message)
						}
					}
				case <-ctx.Done():
					t.Fatal("progress notification missing")
				}
			}
		}
	}
	// A final tokened request is a wire-order barrier: any unintended earlier
	// progress must have arrived before its two expected notifications.
	barrier := &mcp.CallToolParams{Name: "command_wait", Arguments: map[string]any{"task_id": "task-1", "command_id": "command-1"}}
	barrier.SetProgressToken("barrier")
	if _, err = session.CallTool(ctx, barrier); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case got := <-progress:
			if got.ProgressToken != "barrier" {
				t.Fatalf("unexpected progress from a tokenless or non-wait call: %+v", got)
			}
		case <-ctx.Done():
			t.Fatal("barrier progress missing")
		}
	}
	select {
	case got := <-progress:
		t.Fatalf("extra progress notification: %+v", got)
	default:
	}
}

func TestConcurrentToolLimitAndCancelledWaitReleaseSlots(t *testing.T) {
	s := newFake()
	started := make(chan struct{}, 64)
	stopped := make(chan struct{}, 64)
	s.call = func(ctx context.Context, name string, _ json.RawMessage) (model.AgentResult, error) {
		if name != "command_wait" {
			return model.Task{ID: "test-task", State: "running"}, nil
		}
		started <- struct{}{}
		<-ctx.Done()
		stopped <- struct{}{}
		return nil, ctx.Err()
	}
	server := testServer(t, s)
	conn := dial(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client := mcp.NewClient(&mcp.Implementation{Name: "concurrency-test", Version: "1"}, nil)
	session, err := client.Connect(ctx, NewTransport(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	completed := make(chan error, 64)
	cancels := make([]context.CancelFunc, 0, 64)
	for range 64 {
		waitCtx, cancelWait := context.WithCancel(ctx)
		cancels = append(cancels, cancelWait)
		go func() {
			_, err := session.CallTool(waitCtx, &mcp.CallToolParams{Name: "command_wait", Arguments: map[string]any{"task_id": "t", "command_id": "c"}})
			completed <- err
		}()
	}
	defer func() {
		for _, cancelWait := range cancels {
			cancelWait()
		}
	}()
	for range 64 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("64 waits did not start")
		}
	}
	rejected, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "task_get", Arguments: map[string]any{"task_id": "t"}})
	if err != nil || !rejected.IsError || !strings.Contains(rejected.Content[0].(*mcp.TextContent).Text, "too_many_requests") {
		t.Fatalf("65th request was not bounded: %+v err=%v", rejected, err)
	}
	for _, cancelWait := range cancels {
		cancelWait()
	}
	for range 64 {
		select {
		case err := <-completed:
			if err == nil {
				t.Fatal("cancelled wait succeeded")
			}
		case <-ctx.Done():
			t.Fatal("client wait did not cancel")
		}
		select {
		case <-stopped:
		case <-ctx.Done():
			t.Fatal("server wait did not cancel")
		}
	}
	// A successful subsequent call demonstrates that request cancellation
	// releases capacity without cancelling any durable command.
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "task_get", Arguments: map[string]any{"task_id": "t"}})
	if err != nil || result.IsError {
		t.Fatalf("cancelled wait leaked a slot: %+v err=%v", result, err)
	}
}
