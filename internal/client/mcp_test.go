package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/coder/websocket"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func clientMCPResultJSON(t *testing.T, result *mcp.CallToolResult) json.RawMessage {
	t.Helper()
	if result == nil || result.StructuredContent != nil || len(result.Content) != 1 {
		t.Fatalf("expected one text block without structured duplication: %+v", result)
	}
	wire, err := json.Marshal(result)
	if err != nil || len(wire) > model.MaxToolBytes {
		t.Fatalf("unbounded full result: bytes=%d err=%v", len(wire), err)
	}
	content, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("expected JSON text, got %T", result.Content[0])
	}
	if !json.Valid([]byte(content.Text)) {
		t.Fatalf("invalid JSON result: %s", content.Text)
	}
	return json.RawMessage(content.Text)
}

func clientMCPResultValue[Out any](t *testing.T, result *mcp.CallToolResult) Out {
	t.Helper()
	var value Out
	if err := json.Unmarshal(clientMCPResultJSON(t, result), &value); err != nil {
		t.Fatalf("invalid result contract: %v", err)
	}
	return value
}

type clientValueFixture struct {
	Value string `json:"value"`
}

func TestClientToolResultBoundsFullWrapper(t *testing.T) {
	empty := clientToolResult(clientValueFixture{}, nil, "")
	base, err := json.Marshal(empty)
	if err != nil {
		t.Fatal(err)
	}
	fit := strings.Repeat("a", model.MaxToolBytes-len(base))
	for _, tc := range []struct {
		name  string
		value string
		large bool
	}{
		{"exact limit", fit, false},
		{"one byte over", fit + "a", true},
		{"unicode", strings.Repeat("界", 10910), true},
		{"control characters", strings.Repeat("\x00", 4700), true},
		{"quotes and slashes", strings.Repeat("\\\"", 5400), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := clientValueFixture{Value: tc.value}
			payload, err := json.Marshal(input)
			if err != nil || len(payload) > model.MaxToolBytes {
				t.Fatalf("fixture must fit before MCP wrapping: bytes=%d err=%v", len(payload), err)
			}
			result := clientToolResult(input, nil, "")
			if result.IsError != tc.large {
				t.Fatalf("IsError=%v want %v", result.IsError, tc.large)
			}
			if tc.large {
				value := clientMCPResultValue[model.ErrorResult](t, result)
				if value.Error == nil || value.Error.Code != "result_too_large" {
					t.Fatalf("wrong overflow error: %+v", value)
				}
			} else {
				value := clientMCPResultValue[clientValueFixture](t, result)
				if value.Value != tc.value {
					t.Fatal("in-bounds result changed")
				}
			}
		})
	}
}

func TestClientToolResultErrorsKeepDomainFieldsAndTaskID(t *testing.T) {
	domain := &model.Error{Code: "busy", Message: "retry this task", Retryable: true}
	for _, tc := range []struct {
		name string
		err  error
		code string
	}{
		{"domain", fmt.Errorf("wrapped: %w", domain), "busy"},
		{"ordinary", errors.New("secret path and credentials must not be shown"), "internal"},
		{"offline", ErrOffline, "offline"},
		{"deadline", context.DeadlineExceeded, "call_timeout"},
		{"cancelled", context.Canceled, "call_cancelled"},
		{"oversized domain", &model.Error{Code: "busy", Message: strings.Repeat("\x00", 10000)}, "result_too_large"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := clientToolResult(nil, tc.err, "submitted-task")
			value := clientMCPResultValue[model.ErrorResult](t, result)
			detail := value.Error
			if !result.IsError || value.TaskID != "submitted-task" || detail == nil || detail.Code != tc.code {
				t.Fatalf("error lost details: %+v", value)
			}
			if tc.code == "busy" && (detail.Message != domain.Message || !detail.Retryable) {
				t.Fatalf("domain fields changed: %+v", detail)
			}
			if tc.code == "internal" && detail.Message != "operation failed" {
				t.Fatalf("ordinary error exposed details: %+v", detail)
			}
		})
	}
	value := clientMCPResultValue[model.ErrorResult](t, clientToolResult(model.JSON(model.TaskSnapshot{Task: model.Task{ID: "nested-task", Summary: strings.Repeat("x", model.MaxToolBytes)}}), nil, ""))
	if value.TaskID != "nested-task" {
		t.Fatalf("nested submitted ID lost: %+v", value)
	}
	// Even malformed identifiers cannot make the fallback exceed the limit.
	clientMCPResultValue[model.ErrorResult](t, clientToolResult(nil, domain, strings.Repeat("\x00", model.MaxToolBytes)))
}

// Exercise the real RPC path without a command executor or recovery state. The
// peer only answers task RPCs; the MCP side uses the official SDK transports.
func clientMCPTestPeer(t *testing.T, respond func(model.RPC) model.Envelope) (*Client, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		conn.SetReadLimit(model.MaxMessageBytes)
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var message model.Envelope
			var rpc model.RPC
			if json.Unmarshal(data, &message) != nil || message.Type != "rpc" || json.Unmarshal(message.Payload, &rpc) != nil {
				t.Error("invalid test RPC")
				return
			}
			response := respond(rpc)
			response.Type = "response"
			response.RequestID = message.RequestID
			if err := writeEnvelope(ctx, conn, response); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	conn.SetReadLimit(model.MaxMessageBytes)
	c := &Client{cfg: Config{Workspaces: map[string]WorkspaceConfig{"local": {}}}, changed: make(chan struct{}), pending: map[string]chan model.Envelope{}}
	c.live = &liveConnection{conn: conn, ctx: ctx}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var message model.Envelope
			if json.Unmarshal(data, &message) != nil || c.receive(ctx, c.live, message) != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { cancel(); conn.CloseNow(); <-done })
	return c, ctx
}

func clientMCPTestSession(t *testing.T, ctx context.Context, c *Client, options *mcp.ClientOptions) *mcp.ClientSession {
	t.Helper()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := c.MCPServer().Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { serverSession.Close() })
	host := mcp.NewClient(&mcp.Implementation{Name: "test-host", Version: "1"}, options)
	session, err := host.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

func TestClientMCPRunPreservesSubmittedID(t *testing.T) {
	c, ctx := clientMCPTestPeer(t, func(rpc model.RPC) model.Envelope {
		if rpc.Method == "tasks_submit" {
			var args SubmitArgs
			if err := json.Unmarshal(rpc.Args, &args); err != nil {
				t.Error(err)
			}
			return model.Envelope{Payload: model.JSON(model.Task{ID: args.IdempotencyKey})}
		}
		var args WaitArgs
		if err := json.Unmarshal(rpc.Args, &args); err != nil {
			t.Error(err)
		}
		switch args.TaskID {
		case "domain-error":
			return model.Envelope{Error: &model.Error{Code: "busy", Message: "retry wait", Retryable: true}}
		case "invalid-response":
			return model.Envelope{}
		case "oversized":
			return model.Envelope{Payload: model.JSON(clientValueFixture{Value: strings.Repeat("\x00", 4700)})}
		case "no-id":
			return model.Envelope{Payload: json.RawMessage(`{"pending":true}`)}
		default:
			return model.Envelope{Payload: model.JSON(model.TaskWaitResult{TaskID: args.TaskID, Pending: true})}
		}
	})
	session := clientMCPTestSession(t, ctx, c, nil)
	for _, tc := range []struct {
		id   string
		code string
	}{
		{"pending", ""},
		{"no-id", ""},
		{"domain-error", "busy"},
		{"invalid-response", "internal"},
		{"oversized", "result_too_large"},
	} {
		t.Run(tc.id, func(t *testing.T) {
			result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "tasks_run", Arguments: RunArgs{WorkspaceID: "local", Prompt: "work", IdempotencyKey: tc.id, WaitMS: 1}})
			if err != nil {
				t.Fatal(err)
			}
			value := clientMCPResultValue[model.ErrorResult](t, result)
			if value.TaskID != tc.id || result.IsError != (tc.code != "") {
				t.Fatalf("lost task identity or error: %+v", value)
			}
			if tc.code != "" && (value.Error == nil || value.Error.Code != tc.code) {
				t.Fatalf("wrong error: %+v", value)
			}
		})
	}
}

func TestClientRunResultPreservesOpenJSONShape(t *testing.T) {
	for _, tc := range []struct {
		name, response, want string
	}{
		{"missing fields", `{"pending":true}`, `{"task_id":"submitted-task","pending":true}`},
		{"unknown fields and integers", `{"task_id":"wrong-task","future":{"large":9007199254740993},"snapshot":{"future_snapshot":true,"result":18446744073709551615}}`, `{"task_id":"submitted-task","future":{"large":9007199254740993},"snapshot":{"future_snapshot":true,"result":18446744073709551615}}`},
		{"null", `null`, `{"task_id":"submitted-task","result":null}`},
		{"array", `[9007199254740993,{"future":true}]`, `{"task_id":"submitted-task","result":[9007199254740993,{"future":true}]}`},
		{"number", `9007199254740993`, `{"task_id":"submitted-task","result":9007199254740993}`},
		{"string", `"pending"`, `{"task_id":"submitted-task","result":"pending"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := clientRunResult(json.RawMessage(tc.response), "submitted-task")
			if result.IsError {
				t.Fatalf("forwarded result rejected: %+v", result)
			}
			var got, want map[string]json.RawMessage
			if err := json.Unmarshal(clientMCPResultJSON(t, result), &got); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(tc.want), &want); err != nil {
				t.Fatal(err)
			}
			if len(got) != len(want) {
				t.Fatalf("forwarding changed absent fields: got=%s want=%s", model.JSON(got), model.JSON(want))
			}
			for key, expected := range want {
				if string(got[key]) != string(expected) {
					t.Errorf("forwarding changed %q: got=%s want=%s", key, got[key], expected)
				}
			}
		})
	}
	invalid := clientRunResult(nil, "submitted-task")
	value := clientMCPResultValue[model.ErrorResult](t, invalid)
	if !invalid.IsError || value.TaskID != "submitted-task" || value.Error == nil || value.Error.Code != "internal" {
		t.Fatalf("invalid wait result lost identity or error: %+v", value)
	}
}

func TestClientMCPGenericToolsBoundResultsAndErrors(t *testing.T) {
	c, ctx := clientMCPTestPeer(t, func(rpc model.RPC) model.Envelope {
		if rpc.Method == "tasks_cancel" {
			return model.Envelope{Error: &model.Error{Code: "busy", Message: "retry cancel", Retryable: true}}
		}
		task := model.Task{ID: "task-1", Summary: strings.Repeat("\x00", 4700)}
		var value any
		switch rpc.Method {
		case "tasks_submit":
			value = task
		case "tasks_get":
			value = model.TaskSnapshot{Task: task}
		case "tasks_wait":
			value = model.TaskWaitResult{TaskID: task.ID, Snapshot: model.TaskSnapshot{Task: task}}
		case "tasks_list":
			value = model.TaskListResult{Tasks: []model.Task{task}}
		case "tasks_events":
			value = []model.Event{{TaskID: task.ID, Payload: model.JSON(clientValueFixture{Value: task.Summary})}}
		}
		return model.Envelope{Payload: model.JSON(value)}
	})
	session := clientMCPTestSession(t, ctx, c, nil)
	for _, tc := range []struct {
		name string
		args any
		code string
		id   string
	}{
		{"tasks_submit", SubmitArgs{WorkspaceID: "local", Prompt: "work", IdempotencyKey: "task-1"}, "result_too_large", "task-1"},
		{"tasks_get", TaskArgs{TaskID: "task-1"}, "result_too_large", "task-1"},
		{"tasks_wait", WaitArgs{TaskID: "task-1", WaitMS: 1}, "result_too_large", "task-1"},
		{"tasks_cancel", CancelArgs{TaskID: "task-1"}, "busy", "task-1"},
		{"tasks_list", ListArgs{Limit: 1}, "result_too_large", ""},
		{"tasks_events", EventsArgs{TaskID: "task-1", Limit: 1}, "result_too_large", "task-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tc.name, Arguments: tc.args})
			if err != nil {
				t.Fatal(err)
			}
			value := clientMCPResultValue[model.ErrorResult](t, result)
			if !result.IsError || value.TaskID != tc.id || value.Error == nil || value.Error.Code != tc.code {
				t.Fatalf("result lost error or task identity: %+v", value)
			}
		})
	}
}

func TestClientMCPForwardingPreservesDynamicJSONAndBoundsUnknownFields(t *testing.T) {
	const opaque = `{"task":{"task_id":"task-1","result":{"large":9007199254740993}},"extension":{"sequence":18446744073709551615}}`
	c, ctx := clientMCPTestPeer(t, func(rpc model.RPC) model.Envelope {
		var args TaskArgs
		if err := json.Unmarshal(rpc.Args, &args); err != nil {
			t.Error(err)
		}
		if args.TaskID == "oversized" {
			return model.Envelope{Payload: model.JSON(struct {
				Task      model.Task         `json:"task"`
				Extension clientValueFixture `json:"extension"`
			}{Task: model.Task{ID: args.TaskID}, Extension: clientValueFixture{Value: strings.Repeat("\x00", 4700)}})}
		}
		return model.Envelope{Payload: json.RawMessage(opaque)}
	})
	session := clientMCPTestSession(t, ctx, c, nil)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "tasks_get", Arguments: TaskArgs{TaskID: "task-1"}})
	if err != nil || result.IsError {
		t.Fatalf("opaque task result failed: %+v %v", result, err)
	}
	var value struct {
		Task struct {
			Result struct {
				Large json.Number `json:"large"`
			} `json:"result"`
		} `json:"task"`
		Extension struct {
			Sequence json.Number `json:"sequence"`
		} `json:"extension"`
	}
	if err = json.Unmarshal(clientMCPResultJSON(t, result), &value); err != nil {
		t.Fatal(err)
	}
	if value.Task.Result.Large != "9007199254740993" || value.Extension.Sequence != "18446744073709551615" {
		t.Fatalf("opaque fields or integer precision changed: %+v", value)
	}
	result, err = session.CallTool(ctx, &mcp.CallToolParams{Name: "tasks_get", Arguments: TaskArgs{TaskID: "oversized"}})
	if err != nil {
		t.Fatal(err)
	}
	bounded := clientMCPResultValue[model.ErrorResult](t, result)
	if !result.IsError || bounded.TaskID != "oversized" || bounded.Error == nil || bounded.Error.Code != "result_too_large" {
		t.Fatalf("unknown fields bypassed the full result bound: %+v", bounded)
	}
}

func TestClientMCPProgressOnlyForWaitAndRunWithValidToken(t *testing.T) {
	c, ctx := clientMCPTestPeer(t, func(rpc model.RPC) model.Envelope {
		if rpc.Method == "tasks_submit" {
			return model.Envelope{Payload: model.JSON(model.Task{ID: "task-1"})}
		}
		return model.Envelope{Payload: model.JSON(model.TaskWaitResult{TaskID: "task-1", Pending: true})}
	})
	notifications := make(chan *mcp.ProgressNotificationParams, 32)
	session := clientMCPTestSession(t, ctx, c, &mcp.ClientOptions{ProgressNotificationHandler: func(_ context.Context, req *mcp.ProgressNotificationClientRequest) {
		notifications <- req.Params
	}})
	for _, tc := range []struct {
		name  string
		token any
		want  bool
	}{
		{"tasks_wait", "wait-token", true},
		{"tasks_run", 0, true},
		{"tasks_wait", nil, false},
		{"tasks_run", nil, false},
		{"tasks_get", "get-token", false},
		{"tasks_wait", true, false},
		{"tasks_run", mcp.Meta{"bad": "token"}, false},
		{"tasks_wait", strings.Repeat("界", 67), false},
		{"tasks_wait", "", true},
	} {
		args := any(WaitArgs{TaskID: "task-1", WaitMS: 1})
		if tc.name == "tasks_run" {
			args = RunArgs{WorkspaceID: "local", Prompt: "work", IdempotencyKey: "task-1", WaitMS: 1}
		} else if tc.name == "tasks_get" {
			args = TaskArgs{TaskID: "task-1"}
		}
		params := &mcp.CallToolParams{Name: tc.name, Arguments: args}
		if tc.token != nil {
			params.Meta = mcp.Meta{"progressToken": tc.token}
		}
		result, err := session.CallTool(ctx, params)
		if err != nil || result.IsError {
			t.Fatalf("%s: %+v %v", tc.name, result, err)
		}
		clientMCPResultJSON(t, result)
		if tc.want {
			for _, progress := range []float64{0, 1} {
				select {
				case notification := <-notifications:
					if fmt.Sprint(notification.ProgressToken) != fmt.Sprint(tc.token) || notification.Progress != progress || notification.Total != 1 {
						t.Fatalf("incorrect progress for %s: %+v", tc.name, notification)
					}
					if progress == 1 && !strings.Contains(notification.Message, "task may still be running") {
						t.Fatalf("progress claims task completion: %+v", notification)
					}
				case <-time.After(time.Second):
					t.Fatal("missing progress notification")
				}
			}
		}
	}
	select {
	case notification := <-notifications:
		t.Fatalf("unsolicited notification: %+v", notification)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestClientProgressTokenValidation(t *testing.T) {
	for _, token := range []any{nil, true, []any{1}, mcp.Meta{}, strings.Repeat("x", 201), math.NaN(), math.Inf(1), json.Number("NaN"), json.Number("+1"), json.Number("0." + strings.Repeat("0", 200))} {
		if clientProgressTokenValid(token) {
			t.Errorf("accepted invalid token: %v", token)
		}
	}
	for _, token := range []any{"", strings.Repeat("x", 200), 0, -1, 1.25, json.Number("1.5")} {
		if !clientProgressTokenValid(token) {
			t.Errorf("rejected valid token: %v", token)
		}
	}
}
