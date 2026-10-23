package client

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type SubmitArgs struct {
	WorkspaceID    string `json:"workspace_id" jsonschema:"Configured local workspace identifier"`
	Prompt         string `json:"prompt" jsonschema:"Task for the remote Claude Code session"`
	IdempotencyKey string `json:"idempotency_key" jsonschema:"Stable key reused if submission times out or disconnects"`
	DeadlineMS     int64  `json:"deadline_ms,omitempty"`
}
type RunArgs struct {
	WorkspaceID    string `json:"workspace_id"`
	Prompt         string `json:"prompt"`
	IdempotencyKey string `json:"idempotency_key"`
	DeadlineMS     int64  `json:"deadline_ms,omitempty"`
	WaitMS         int64  `json:"wait_ms,omitempty"`
}
type TaskArgs struct {
	TaskID string `json:"task_id"`
}
type WaitArgs struct {
	TaskID string `json:"task_id"`
	WaitMS int64  `json:"wait_ms,omitempty"`
}
type CancelArgs struct {
	TaskID string `json:"task_id"`
	Reason string `json:"reason,omitempty"`
}
type ListArgs struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	Cursor      int64  `json:"cursor,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}
type EventsArgs struct {
	TaskID   string `json:"task_id"`
	AfterSeq int64  `json:"after_seq,omitempty"`
	Limit    int    `json:"limit,omitempty"`
}

// MCPServer provides only task tools. Its handlers can wait concurrently while
// the independent outbound connection continues executing remote commands.
func (c *Client) MCPServer() *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "task-bridge-client", Version: model.Version}, nil)
	addClientTool(server, c, "tasks_submit", "Submit a durable task. Reuse idempotency_key when retrying the same task.", func(args SubmitArgs) error { return c.validateWorkspace(args.WorkspaceID) })
	addClientTool(server, c, "tasks_get", "Get durable task status and the final result.", func(TaskArgs) error { return nil })
	addClientTool(server, c, "tasks_wait", "Wait up to 60 seconds. Timeout or cancellation of this call does not cancel the task.", validateWait)
	addClientTool(server, c, "tasks_cancel", "Request cancellation; offline commands may still need stop confirmation.", func(CancelArgs) error { return nil })
	addClientTool(server, c, "tasks_list", "List tasks owned by this executor.", func(args ListArgs) error {
		if args.WorkspaceID != "" {
			return c.validateWorkspace(args.WorkspaceID)
		}
		return nil
	})
	addClientTool(server, c, "tasks_events", "Read durable progress events after a cursor.", func(EventsArgs) error { return nil })
	mcp.AddTool(server, &mcp.Tool{Name: "tasks_run", Description: "Submit and wait for a task. Returns task_id even when the bounded wait expires; query that task instead of creating another."}, func(ctx context.Context, req *mcp.CallToolRequest, args RunArgs) (*mcp.CallToolResult, any, error) {
		ctx, cancel := context.WithTimeout(ctx, 80*time.Second)
		defer cancel()
		if err := c.validateWorkspace(args.WorkspaceID); err != nil {
			return clientToolResult(nil, err, ""), nil, nil
		}
		if err := validateWait(WaitArgs{WaitMS: args.WaitMS}); err != nil {
			return clientToolResult(nil, err, ""), nil, nil
		}
		defer clientCallProgress(ctx, req)()
		response, err := c.RPC(ctx, "tasks_submit", SubmitArgs{WorkspaceID: args.WorkspaceID, Prompt: args.Prompt, IdempotencyKey: args.IdempotencyKey, DeadlineMS: args.DeadlineMS})
		if err != nil {
			return clientToolResult(nil, err, ""), nil, nil
		}
		var submitted map[string]any
		if err = json.Unmarshal(response, &submitted); err != nil {
			return clientToolResult(nil, err, ""), nil, nil
		}
		id := clientTaskID(submitted)
		if id == "" {
			return clientToolResult(submitted, nil, ""), nil, nil
		}
		response, err = c.RPC(ctx, "tasks_wait", WaitArgs{TaskID: id, WaitMS: args.WaitMS})
		if err != nil {
			return clientToolResult(nil, err, id), nil, nil
		}
		var output any
		if err = json.Unmarshal(response, &output); err != nil {
			return clientToolResult(nil, err, id), nil, nil
		}
		if object, ok := output.(map[string]any); ok {
			object["task_id"] = id
		} else {
			output = map[string]any{"task_id": id, "result": output}
		}
		return clientToolResult(output, nil, id), nil, nil
	})
	return server
}

func addClientTool[In any](server *mcp.Server, c *Client, name, description string, validate func(In) error) {
	mcp.AddTool(server, &mcp.Tool{Name: name, Description: description}, func(ctx context.Context, req *mcp.CallToolRequest, args In) (*mcp.CallToolResult, any, error) {
		id := clientTaskID(args)
		if err := validate(args); err != nil {
			return clientToolResult(nil, err, id), nil, nil
		}
		// Per-call bounds prevent a broken broker from retaining unlimited waiters.
		ctx, cancel := context.WithTimeout(ctx, 70*time.Second)
		defer cancel()
		if name == "tasks_wait" {
			defer clientCallProgress(ctx, req)()
		}
		data, err := c.RPC(ctx, name, args)
		if err != nil {
			return clientToolResult(nil, err, id), nil, nil
		}
		var output any
		if err = json.Unmarshal(data, &output); err != nil {
			return clientToolResult(nil, err, id), nil, nil
		}
		return clientToolResult(output, nil, id), nil, nil
	})
}

func clientTaskID(value any) string {
	switch v := value.(type) {
	case TaskArgs:
		return v.TaskID
	case WaitArgs:
		return v.TaskID
	case CancelArgs:
		return v.TaskID
	case EventsArgs:
		return v.TaskID
	case map[string]any:
		if id, _ := v["task_id"].(string); id != "" {
			return id
		}
		if task, ok := v["task"].(map[string]any); ok {
			id, _ := task["task_id"].(string)
			return id
		}
	}
	return ""
}

func clientToolResult(value any, err error, taskID string) *mcp.CallToolResult {
	if taskID == "" {
		taskID = clientTaskID(value)
	}
	if err != nil {
		var domainError *model.Error
		if !errors.As(err, &domainError) || domainError == nil {
			switch {
			case errors.Is(err, ErrOffline):
				domainError = &model.Error{Code: "offline", Message: ErrOffline.Error(), Retryable: true}
			case errors.Is(err, context.DeadlineExceeded):
				domainError = &model.Error{Code: "call_timeout", Message: "tool call timed out; a submitted task may still be running", Retryable: true}
			case errors.Is(err, context.Canceled):
				domainError = &model.Error{Code: "call_cancelled", Message: "tool call was cancelled; this does not cancel the task", Retryable: true}
			default:
				domainError = &model.Error{Code: "internal", Message: "operation failed"}
			}
		}
		value = clientErrorValue(domainError, taskID)
	}
	data, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		return clientSmallError("internal", "tool result could not be serialized", taskID)
	}
	result := &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(data)}}, IsError: err != nil}
	// JSON text escaping and the MCP wrapper both count toward the limit.
	// Returning nil typed output avoids the SDK duplicating StructuredContent.
	wire, marshalErr := json.Marshal(result)
	if marshalErr != nil || len(wire) > model.MaxToolBytes {
		return clientSmallError("result_too_large", "result exceeds 32 KiB; request a smaller page or summary", taskID)
	}
	return result
}

func clientErrorValue(err *model.Error, taskID string) map[string]any {
	value := map[string]any{"error": err}
	if taskID != "" {
		value["task_id"] = taskID
	}
	return value
}

func clientSmallError(code, message, taskID string) *mcp.CallToolResult {
	value := clientErrorValue(&model.Error{Code: code, Message: message}, taskID)
	result := &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(model.JSON(value))}}, IsError: true}
	if wire, err := json.Marshal(result); err != nil || len(wire) > model.MaxToolBytes {
		// A malicious or corrupt task ID must not defeat the final bound.
		delete(value, "task_id")
		result.Content = []mcp.Content{&mcp.TextContent{Text: string(model.JSON(value))}}
	}
	return result
}

// Progress describes this bounded tool call, never completion of the task.
func clientCallProgress(ctx context.Context, req *mcp.CallToolRequest) func() {
	if req == nil || req.Params == nil || req.Session == nil {
		return func() {}
	}
	token := req.Params.GetProgressToken()
	if !clientProgressTokenValid(token) {
		return func() {}
	}
	notify := func(progress float64, message string) {
		// A cancelled wait can still report that this call ended, but a slow
		// or disconnected host must never retain the handler indefinitely.
		notifyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		_ = req.Session.NotifyProgress(notifyCtx, &mcp.ProgressNotificationParams{ProgressToken: token, Progress: progress, Total: 1, Message: message})
	}
	notify(0, "Tool call started; task progress is reported separately.")
	return func() { notify(1, "Tool call completed; the task may still be running.") }
}

func clientProgressTokenValid(token any) bool {
	switch v := token.(type) {
	case string:
		return len(v) <= 200
	case float64:
		return !math.IsNaN(v) && !math.IsInf(v, 0)
	case float32:
		return !math.IsNaN(float64(v)) && !math.IsInf(float64(v), 0)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	case json.Number:
		n, err := v.Float64()
		return len(v) <= 200 && json.Valid([]byte(v)) && err == nil && !math.IsNaN(n) && !math.IsInf(n, 0)
	}
	return false
}

func (c *Client) validateWorkspace(id string) error {
	if _, ok := c.cfg.Workspaces[id]; !ok {
		return model.Err("invalid_arguments", "workspace_id is not configured on this client")
	}
	return nil
}
func validateWait(args WaitArgs) error {
	if args.WaitMS < 0 || args.WaitMS > 60000 {
		return model.Err("invalid_arguments", "wait_ms must be between 0 and 60000")
	}
	return nil
}
