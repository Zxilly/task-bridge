package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func boundsTestSession(t *testing.T, server *mcp.Server) *mcp.ClientSession {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = serverSession.Close() })
	client := mcp.NewClient(&mcp.Implementation{Name: "bounds-test", Version: "1"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func TestClientSDKValidationErrorRespectsResultBound(t *testing.T) {
	// This rejected argument never reaches the typed tool handler. Exercise
	// the application's actual SDK server, including its receiving middleware.
	session := boundsTestSession(t, (&Client{}).MCPServer())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, tc := range []struct {
		name, id, wantID string
	}{
		{"known identity", "known-task", "known-task"},
		{"oversized identity", strings.Repeat("\x00", model.MaxToolBytes), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := session.CallTool(ctx, &mcp.CallToolParams{
				Name: "tasks_get",
				Arguments: map[string]json.RawMessage{
					"task_id":                               model.JSON(tc.id),
					strings.Repeat("x", model.MaxToolBytes): json.RawMessage(`true`),
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			assertBoundedSDKError(t, result, "result_too_large", tc.wantID)
		})
	}
}

func assertBoundedSDKError(t *testing.T, result *mcp.CallToolResult, code, taskID string) {
	t.Helper()
	wire, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) > model.MaxToolBytes {
		t.Fatalf("SDK result bypassed the tool result bound: got %d bytes, limit %d", len(wire), model.MaxToolBytes)
	}
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
		TaskID string `json:"task_id"`
	}
	if !result.IsError || result.StructuredContent != nil || len(result.Content) != 1 {
		t.Fatalf("unexpected bounded error result: %+v", result)
	}
	if err = json.Unmarshal([]byte(result.Content[0].(*mcp.TextContent).Text), &body); err != nil || body.Error.Code != code || body.TaskID != taskID {
		t.Fatalf("bounded SDK error lost code or task identity: %+v err=%v", body, err)
	}
}

func TestClientSDKResultBoundsPreserveBoundaries(t *testing.T) {
	textResult := func(text string) *mcp.CallToolResult {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}
	}
	// Include fields the negotiated SDK protocol adds during the real call.
	base, err := json.Marshal(boundsTestRoundTripResult(t, textResult(""), true))
	if err != nil {
		t.Fatal(err)
	}
	capacity := model.MaxToolBytes - len(base)
	unicodeFit := strings.Repeat("界", capacity/3) + strings.Repeat("x", capacity%3)
	for _, tc := range []struct {
		name   string
		result *mcp.CallToolResult
		code   string
	}{
		{"ordinary result", textResult("ready"), ""},
		{"ordinary tool error", &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "known failure"}}, IsError: true}, ""},
		{"exact limit", textResult(strings.Repeat("a", capacity)), ""},
		{"one byte over", textResult(strings.Repeat("a", capacity+1)), "result_too_large"},
		{"unicode exact limit", textResult(unicodeFit), ""},
		{"unicode one byte over", textResult(unicodeFit + "x"), "result_too_large"},
		{"JSON escaping counts", textResult(strings.Repeat("\x00", capacity/6+1)), "result_too_large"},
		{"unserializable result", &mcp.CallToolResult{StructuredContent: make(chan int)}, "internal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := boundsTestRoundTripResult(t, tc.result, true)
			if tc.code != "" {
				assertBoundedSDKError(t, result, tc.code, "known-task")
				return
			}
			got, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			// Compare with the SDK's final response without the size middleware.
			want, err := json.Marshal(boundsTestRoundTripResult(t, tc.result, false))
			if err != nil {
				t.Fatal(err)
			}
			if len(got) > model.MaxToolBytes || !bytes.Equal(got, want) {
				t.Fatalf("in-bounds result changed: got %d bytes, want %d", len(got), len(want))
			}
		})
	}
}

func boundsTestRoundTripResult(t *testing.T, output *mcp.CallToolResult, bounded bool) *mcp.CallToolResult {
	t.Helper()
	implementation := &mcp.Implementation{Name: "bounds-server", Version: "1"}
	server := mcp.NewServer(implementation, nil)
	if bounded {
		addClientResultBounds(server, implementation)
	}
	mcp.AddTool(server, &mcp.Tool{Name: "probe"}, func(context.Context, *mcp.CallToolRequest, TaskArgs) (*mcp.CallToolResult, any, error) {
		return output, nil, nil
	})
	session := boundsTestSession(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "probe", Arguments: TaskArgs{TaskID: "known-task"}})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestClientSDKResultBoundsPreserveLifecycleAndProtocolErrors(t *testing.T) {
	large := strings.Repeat("x", model.MaxToolBytes+1)
	implementation := &mcp.Implementation{Name: "bounds-server", Version: "1"}
	server := mcp.NewServer(implementation, &mcp.ServerOptions{Instructions: large})
	addClientResultBounds(server, implementation)
	mcp.AddTool(server, &mcp.Tool{Name: "probe", Description: large}, func(context.Context, *mcp.CallToolRequest, TaskArgs) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{}, nil, nil
	})
	session := boundsTestSession(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if got := session.InitializeResult().Instructions; got != large {
		t.Fatalf("non-tool initialize result was changed: got %d bytes, want %d", len(got), len(large))
	}
	list, err := session.ListTools(ctx, nil)
	if err != nil || len(list.Tools) != 1 || list.Tools[0].Description != large {
		t.Fatalf("non-tool list result was changed: err=%v", err)
	}
	if err = session.Ping(ctx, nil); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "missing", Arguments: struct{}{}})
	var protocolError *jsonrpc.Error
	if result != nil || !errors.As(err, &protocolError) || protocolError.Code != jsonrpc.CodeInvalidParams || protocolError.Message != `unknown tool "missing"` {
		t.Fatalf("SDK protocol error changed: result=%+v err=%v", result, err)
	}
}
