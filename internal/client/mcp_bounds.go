package client

import (
	"context"
	"encoding/json"
	"maps"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// addClientResultBounds also covers errors returned by the SDK before a typed
// handler runs, such as schema validation errors containing unknown field names.
func addClientResultBounds(server *mcp.Server, implementation *mcp.Implementation) {
	server.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			result, err := next(ctx, method, req)
			if err != nil || method != "tools/call" {
				return result, err
			}
			toolResult, ok := result.(*mcp.CallToolResult)
			if !ok || toolResult == nil {
				return result, nil
			}
			var taskID string
			var includeServerInfo bool
			if call, ok := req.(*mcp.CallToolRequest); ok && call.Params != nil {
				taskID = clientTaskID(call.Params.Arguments)
				version, _ := call.Params.GetMeta()[mcp.MetaKeyProtocolVersion].(string)
				includeServerInfo = version >= "2026-07-28"
			}
			withServerInfo := func(value *mcp.CallToolResult) *mcp.CallToolResult {
				if !includeServerInfo {
					return value
				}
				return clientResultWithServerInfo(value, implementation)
			}
			toolResult = withServerInfo(toolResult)
			wire, marshalErr := json.Marshal(toolResult)
			if marshalErr == nil && len(wire) <= model.MaxToolBytes {
				return toolResult, nil
			}
			code, message := "result_too_large", "result exceeds 32 KiB; request a smaller page or summary"
			if marshalErr != nil {
				code, message = "internal", "tool result could not be serialized"
			}
			for _, id := range []string{taskID, ""} {
				bounded := withServerInfo(clientSmallError(code, message, id))
				if wire, err := json.Marshal(bounded); err == nil && len(wire) <= model.MaxToolBytes {
					return bounded, nil
				}
			}
			return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "tool result metadata exceeds limit"}
		}
	})
}

func clientResultWithServerInfo(result *mcp.CallToolResult, implementation *mcp.Implementation) *mcp.CallToolResult {
	// The SDK annotates new-protocol responses after receiving middleware.
	// Populate its public metadata entry first so the measured result is final;
	// the SDK preserves an existing entry, including one supplied by a handler.
	metadata := result.GetMeta()
	if _, exists := metadata[mcp.MetaKeyServerInfo]; exists {
		return result
	}
	metadata = maps.Clone(metadata)
	if metadata == nil {
		metadata = mcp.Meta{}
	}
	metadata[mcp.MetaKeyServerInfo] = implementation
	copy := *result
	copy.SetMeta(metadata)
	return &copy
}
