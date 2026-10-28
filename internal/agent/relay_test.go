package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func relayTestOffer(task, assignment string) model.ChannelNotification {
	return model.ChannelNotification{Meta: model.ChannelMetadata{TaskID: task, AssignmentID: assignment, WorkspaceID: "workspace"}}
}

func relayTestUpstream(t *testing.T, handler mcp.ToolHandler) (*mcp.ClientSession, []*mcp.Tool) {
	t.Helper()
	server := mcp.NewServer(&mcp.Implementation{Name: "relay-upstream", Version: "1"}, nil)
	for _, name := range []string{"agent_usage_update", "task_accept", "task_get", "task_resume", "bash", "command_get", "command_wait", "command_output", "command_cancel", "task_progress", "task_complete", "unrelated"} {
		server.AddTool(&mcp.Tool{Name: name, InputSchema: map[string]any{"type": "object"}}, handler)
	}
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(t.Context(), serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = serverSession.Close() })
	client := mcp.NewClient(&mcp.Implementation{Name: "relay-controller-test", Version: "1"}, nil)
	session, err := client.Connect(t.Context(), clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	list, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return session, list.Tools
}

func relayTestNew(t *testing.T, ctx context.Context, upstream *mcp.ClientSession, tools []*mcp.Tool, offer model.ChannelNotification) *taskRelay {
	t.Helper()
	r, err := newRelay(ctx, upstream, offer, tools)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Errorf("close relay: %v", err)
		}
	})
	return r
}

type relayBearerTransport struct {
	base  http.RoundTripper
	token string
}

func (t relayBearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	copy := req.Clone(req.Context())
	copy.Header.Set("Authorization", "Bearer "+t.token)
	return t.base.RoundTrip(copy)
}

func relayTestClient(t *testing.T, relay *taskRelay, version string) *mcp.ClientSession {
	t.Helper()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	t.Cleanup(transport.CloseIdleConnections)
	client := mcp.NewClient(&mcp.Implementation{Name: "relay-child-test", Version: "1"}, nil)
	session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{
		Endpoint: relay.URL(), HTTPClient: &http.Client{Transport: relayBearerTransport{base: transport, token: relay.Token()}},
		DisableStandaloneSSE: true, MaxRetries: -1,
	}, &mcp.ClientSessionOptions{ProtocolVersion: version})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func relayTestError(t *testing.T, result *mcp.CallToolResult, code string) {
	t.Helper()
	if result == nil || !result.IsError || len(result.Content) != 1 {
		t.Fatalf("expected bounded tool error %q, got %+v", code, result)
	}
	text, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("unexpected error content: %T", result.Content[0])
	}
	var body model.ErrorResult
	if err := json.Unmarshal([]byte(text.Text), &body); err != nil || body.Error == nil || body.Error.Code != code {
		t.Fatalf("expected %q, got %s, err=%v", code, text.Text, err)
	}
	wire, err := json.Marshal(result)
	if err != nil || len(wire) > model.MaxToolBytes {
		t.Fatalf("tool error exceeds bound: size=%d, err=%v", len(wire), err)
	}
}

func TestRelayScopesEveryToolAndPreservesRawArguments(t *testing.T) {
	var calls atomic.Int32
	arguments := make(chan json.RawMessage, 32)
	upstream, tools := relayTestUpstream(t, func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		calls.Add(1)
		arguments <- append(json.RawMessage(nil), req.Params.Arguments...)
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil
	})
	r := relayTestNew(t, t.Context(), upstream, tools, relayTestOffer("task-one", "assignment-one"))
	session := relayTestClient(t, r, "")
	list, err := session.ListTools(t.Context(), nil)
	if err != nil || len(list.Tools) != 9 {
		t.Fatalf("task tool list: count=%d err=%v", len(list.Tools), err)
	}
	for _, tool := range list.Tools {
		if tool.Name == "agent_usage_update" || tool.Name == "task_resume" || tool.Name == "unrelated" {
			t.Fatalf("unscoped tool exposed: %s", tool.Name)
		}
		result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: tool.Name, Arguments: json.RawMessage(`{"task_id":"other-task"}`)})
		if err != nil {
			t.Fatal(err)
		}
		relayTestError(t, result, "invalid_arguments")
	}
	for _, raw := range []string{
		`{"task_id":"task-one","assignment_id":"old-assignment"}`,
		`{"task_id":"task-one","assignment_id":null}`,
		`{"task_id":"task-one","assignment_id":123}`,
		`{"task_id":"task-one","TASK_ID":"other-task"}`,
		`{"task_id":"task-one","assignment_id":"assignment-one","ASSIGNMENT_ID":"other"}`,
		`{"task_id":"task-one","task_id":"task-one"}`,
		`{"task_id":"task-one","assignment_id":"assignment-one","assignment_id":"assignment-one"}`,
		`{"task_id":null}`, `{"task_id":42}`, `{}`,
	} {
		result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "task_get", Arguments: json.RawMessage(raw)})
		if err != nil {
			t.Fatal(err)
		}
		relayTestError(t, result, "invalid_arguments")
	}
	for _, name := range []string{"agent_usage_update", "task_resume", "unrelated"} {
		_, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: name, Arguments: json.RawMessage(`{"task_id":"task-one"}`)})
		var protocolError *jsonrpc.Error
		if !errors.As(err, &protocolError) || protocolError.Code != jsonrpc.CodeInvalidParams {
			t.Fatalf("unlisted tool %s was not rejected: %v", name, err)
		}
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("rejected calls reached upstream: %d", got)
	}
	raw := json.RawMessage(`{"task_id":"task-one","assignment_id":"assignment-one","command":"printf ok","operation_key":"once","nested":[9007199254740993,1e50,[],{"value":-9007199254740993}]}`)
	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "bash", Arguments: raw})
	if err != nil || result.IsError {
		t.Fatalf("valid call: result=%+v err=%v", result, err)
	}
	if got := <-arguments; !bytes.Equal(got, raw) {
		t.Fatalf("arguments were rebuilt:\n got %s\nwant %s", got, raw)
	}
	if calls.Load() != 1 {
		t.Fatalf("unexpected upstream calls: %d", calls.Load())
	}
}

func TestRelayPreservesResultsAndProtocolErrors(t *testing.T) {
	output := &mcp.CallToolResult{
		Content:           []mcp.Content{&mcp.TextContent{Text: `{"error":{"code":"command_failed","retryable":false}}`}},
		StructuredContent: map[string]any{"nested": []any{"a", true, nil}}, IsError: true,
	}
	output.SetMeta(mcp.Meta{"custom": map[string]any{"source": "upstream"}})
	upstream, tools := relayTestUpstream(t, func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if req.Params.Name == "command_get" {
			return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "upstream rejected", Data: json.RawMessage(`{"detail":"preserved"}`)}
		}
		return output, nil
	})
	r := relayTestNew(t, t.Context(), upstream, tools, relayTestOffer("task-one", "assignment-one"))
	session := relayTestClient(t, r, "")
	params := &mcp.CallToolParams{Name: "task_get", Arguments: json.RawMessage(`{"task_id":"task-one"}`)}
	want, err := upstream.CallTool(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}
	got, err := session.CallTool(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, _ := json.Marshal(want)
	gotJSON, _ := json.Marshal(got)
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("relay changed SDK tool result:\n got %s\nwant %s", gotJSON, wantJSON)
	}
	// The new protocol represents method errors as HTTP transport errors. The
	// current broker/Claude protocol preserves the original JSON-RPC envelope.
	legacySession := relayTestClient(t, r, "2025-11-25")
	_, err = legacySession.CallTool(t.Context(), &mcp.CallToolParams{Name: "command_get", Arguments: params.Arguments})
	var protocolError *jsonrpc.Error
	if !errors.As(err, &protocolError) || protocolError.Code != jsonrpc.CodeInvalidParams || protocolError.Message != "upstream rejected" || string(protocolError.Data) != `{"detail":"preserved"}` {
		t.Fatalf("upstream protocol error changed: err=%v protocol=%#v", err, protocolError)
	}
}

func TestRelayBoundsFinalSDKResponse(t *testing.T) {
	var output atomic.Pointer[mcp.CallToolResult]
	setText := func(text string) {
		output.Store(&mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}})
	}
	setText("")
	upstream, tools := relayTestUpstream(t, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) { return output.Load(), nil })
	r := relayTestNew(t, t.Context(), upstream, tools, relayTestOffer("task-one", "assignment-one"))
	session := relayTestClient(t, r, "")
	params := &mcp.CallToolParams{Name: "task_get", Arguments: json.RawMessage(`{"task_id":"task-one"}`)}
	baseline, err := session.CallTool(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := json.Marshal(baseline)
	capacity := model.MaxToolBytes - len(wire)
	for _, tc := range []struct {
		name, text string
		oversized  bool
	}{
		{"exact boundary", strings.Repeat("x", capacity), false},
		{"one byte over", strings.Repeat("x", capacity+1), true},
		{"escaped content", strings.Repeat("\x00", capacity/6+1), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setText(tc.text)
			result, err := session.CallTool(t.Context(), params)
			if err != nil {
				t.Fatal(err)
			}
			if tc.oversized {
				relayTestError(t, result, "result_too_large")
				return
			}
			wire, err := json.Marshal(result)
			if err != nil || len(wire) != model.MaxToolBytes || result.IsError || result.Content[0].(*mcp.TextContent).Text != tc.text {
				t.Fatalf("exact response boundary changed: len=%d err=%v", len(wire), err)
			}
		})
	}
}

func relayTestHTTP(t *testing.T, endpoint, token, body string, configure func(*http.Request)) int {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	request.Header.Set("MCP-Protocol-Version", "2025-11-25")
	if configure != nil {
		configure(request)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode
}

func TestRelayHTTPAuthorizationOriginAndBodyBound(t *testing.T) {
	var calls atomic.Int32
	upstream, tools := relayTestUpstream(t, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		calls.Add(1)
		return &mcp.CallToolResult{}, nil
	})
	r := relayTestNew(t, t.Context(), upstream, tools, relayTestOffer("task-one", "assignment-one"))
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"task_get","arguments":{"task_id":"task-one"}}}`
	for _, tc := range []struct {
		name      string
		token     string
		configure func(*http.Request)
		status    int
	}{
		{"wrong token", "wrong", nil, http.StatusUnauthorized},
		{"missing token", r.Token(), func(r *http.Request) { r.Header.Del("Authorization") }, http.StatusUnauthorized},
		{"duplicate token", r.Token(), func(r *http.Request) { r.Header.Add("Authorization", r.Header.Get("Authorization")) }, http.StatusUnauthorized},
		{"cross origin", r.Token(), func(r *http.Request) { r.Header.Set("Origin", "https://untrusted.example") }, http.StatusForbidden},
		{"DNS rebinding", r.Token(), func(r *http.Request) { r.Host = "untrusted.example" }, http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if status := relayTestHTTP(t, r.URL(), tc.token, body, tc.configure); status != tc.status {
				t.Fatalf("status=%d want=%d", status, tc.status)
			}
		})
	}
	large := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"task_get","arguments":{"task_id":"task-one","large":"` + strings.Repeat("x", model.MaxMessageBytes) + `"}}}`
	if status := relayTestHTTP(t, r.URL(), r.Token(), large, nil); status != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body status=%d", status)
	}
	if calls.Load() != 0 {
		t.Fatalf("rejected HTTP calls reached upstream: %d", calls.Load())
	}
	if status := relayTestHTTP(t, r.URL(), r.Token(), body, nil); status != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("valid request failed: status=%d calls=%d", status, calls.Load())
	}
}

func TestRelayRequestCancellationReachesUpstream(t *testing.T) {
	for _, version := range []string{"2025-11-25", "2026-07-28"} {
		t.Run(version, func(t *testing.T) {
			started, cancelled := make(chan struct{}), make(chan struct{})
			upstream, tools := relayTestUpstream(t, func(ctx context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				close(started)
				<-ctx.Done()
				close(cancelled)
				return nil, ctx.Err()
			})
			r := relayTestNew(t, t.Context(), upstream, tools, relayTestOffer("task-one", "assignment-one"))
			session := relayTestClient(t, r, version)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			returned := make(chan error, 1)
			go func() {
				_, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "command_wait", Arguments: json.RawMessage(`{"task_id":"task-one","command_id":"command"}`)})
				returned <- err
			}()
			relayTestSignal(t, started, "upstream call started")
			cancel()
			relayTestSignal(t, cancelled, "upstream call cancelled")
			select {
			case err := <-returned:
				if err == nil {
					t.Fatal("cancelled call returned no error")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("cancelled call did not return")
			}
		})
	}
}

func relayTestSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for " + description)
	}
}

func TestRelayCloseCancelsAndRevokesOldClient(t *testing.T) {
	started, cancelled := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	upstream, tools := relayTestUpstream(t, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		calls.Add(1)
		if req.Params.Name == "command_wait" {
			close(started)
			<-ctx.Done()
			close(cancelled)
			return nil, ctx.Err()
		}
		return &mcp.CallToolResult{}, nil
	})
	r := relayTestNew(t, t.Context(), upstream, tools, relayTestOffer("task-one", "assignment-one"))
	oldClient := relayTestClient(t, r, "")
	callDone := make(chan struct{})
	go func() {
		defer close(callDone)
		_, _ = oldClient.CallTool(t.Context(), &mcp.CallToolParams{Name: "command_wait", Arguments: json.RawMessage(`{"task_id":"task-one","command_id":"command"}`)})
	}()
	relayTestSignal(t, started, "upstream call started")
	closed := make(chan struct{})
	go func() { _ = r.Close(); close(closed) }()
	relayTestSignal(t, cancelled, "upstream cancellation after close")
	relayTestSignal(t, closed, "relay joined handlers")
	relayTestSignal(t, callDone, "old client returned")
	if err := upstream.Ping(t.Context(), nil); err != nil {
		t.Fatalf("closing one task closed controller upstream: %v", err)
	}
	_, err := oldClient.CallTool(t.Context(), &mcp.CallToolParams{Name: "task_get", Arguments: json.RawMessage(`{"task_id":"task-one"}`)})
	if err == nil || calls.Load() != 1 {
		t.Fatalf("old client revived after close: err=%v calls=%d", err, calls.Load())
	}
	next := relayTestNew(t, t.Context(), upstream, tools, relayTestOffer("task-two", "assignment-two"))
	if next.Token() == r.Token() {
		t.Fatal("task relays reused authorization token")
	}
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"task_get","arguments":{"task_id":"task-two"}}}`
	if status := relayTestHTTP(t, next.URL(), r.Token(), body, nil); status != http.StatusUnauthorized {
		t.Fatalf("old token accepted by new relay: status=%d", status)
	}
	nextClient := relayTestClient(t, next, "")
	if _, err := nextClient.CallTool(t.Context(), &mcp.CallToolParams{Name: "task_get", Arguments: json.RawMessage(`{"task_id":"task-two"}`)}); err != nil || calls.Load() != 2 {
		t.Fatalf("new task unavailable: err=%v calls=%d", err, calls.Load())
	}
}

func TestRelayParentCancellationRevokesEndpoint(t *testing.T) {
	upstream, tools := relayTestUpstream(t, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{}, nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	r := relayTestNew(t, ctx, upstream, tools, relayTestOffer("task-one", "assignment-one"))
	cancel()
	relayTestSignal(t, r.done, "parent cancellation closed relay")
	if r.enter() {
		r.inflight.Done()
		t.Fatal("cancelled relay still admits calls")
	}
	if err := upstream.Ping(t.Context(), nil); err != nil {
		t.Fatalf("parent task cancellation closed shared upstream: %v", err)
	}
}
