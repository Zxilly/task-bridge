package client

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/Zxilly/task-bridge/internal/mcputil"
	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type SubmitArgs = model.TaskSubmitArgs
type RunArgs = model.TaskRunArgs
type ResumeArgs = model.TaskResumeArgs
type TaskArgs = model.TaskGetArgs
type WaitArgs = model.TaskWaitArgs
type CancelArgs = model.TaskCancelArgs
type ListArgs = model.TaskListArgs
type EventsArgs = model.TaskEventsArgs

type clientTaskRequest interface {
	model.ClientRequest
	TaskIdentifier() string
}

// MCPServer provides only task tools. Its handlers can wait concurrently while
// the independent outbound connection continues executing remote commands.
func (c *Client) MCPServer() *mcp.Server {
	implementation := &mcp.Implementation{Name: "task-bridge-client", Version: model.Version}
	server := mcp.NewServer(implementation, nil)
	addClientResultBounds(server, implementation)
	addClientTool(server, c, "Submit a durable task. Prompt must be English only (ASCII): printable ASCII plus TAB/LF/CR and at least one A-Z or a-z letter. Reuse idempotency_key when retrying the same task.", func(args SubmitArgs) error { return c.validateWorkspace(args.WorkspaceID) })
	addClientTool(server, c, "Get durable task status and the final result.", func(TaskArgs) error { return nil })
	addClientTool(server, c, "Request durable recovery of this same task's original Claude conversation on A. Reuse idempotency_key when retrying this recovery request; a different key is rejected while a request is pending. A pending or accepted receipt does not mean recovery succeeded or the task completed. Use tasks_get or tasks_wait for task status and the durable final result; unresolved execution still requires reconciliation.", func(ResumeArgs) error { return nil })
	addClientTool(server, c, "Wait up to 60 seconds. Timeout or cancellation of this call does not cancel the task.", validateWait)
	addClientTool(server, c, "Request cancellation; offline commands may still need stop confirmation.", func(CancelArgs) error { return nil })
	addClientTool(server, c, "List tasks owned by this executor.", func(args ListArgs) error {
		if args.WorkspaceID != "" {
			return c.validateWorkspace(args.WorkspaceID)
		}
		return nil
	})
	addClientTool(server, c, "Read durable progress events after a cursor.", func(EventsArgs) error { return nil })
	mcp.AddTool(server, &mcp.Tool{Name: "tasks_run", Description: "Submit and wait for a task. Prompt must be English only (ASCII): printable ASCII plus TAB/LF/CR and at least one A-Z or a-z letter. Returns task_id even when the bounded wait expires; query that task instead of creating another."}, func(ctx context.Context, req *mcp.CallToolRequest, args RunArgs) (*mcp.CallToolResult, any, error) {
		ctx, cancel := context.WithTimeout(ctx, 80*time.Second)
		defer cancel()
		if err := c.validateWorkspace(args.WorkspaceID); err != nil {
			return clientToolResult(nil, err, ""), nil, nil
		}
		if err := validateWait(WaitArgs{WaitMS: args.WaitMS}); err != nil {
			return clientToolResult(nil, err, ""), nil, nil
		}
		defer clientCallProgress(ctx, req)()
		response, err := c.RPC(ctx, SubmitArgs{WorkspaceID: args.WorkspaceID, Prompt: args.Prompt, IdempotencyKey: args.IdempotencyKey, DeadlineMS: args.DeadlineMS})
		if err != nil {
			return clientToolResult(nil, err, ""), nil, nil
		}
		var submitted model.Task
		if err = json.Unmarshal(response, &submitted); err != nil {
			return clientToolResult(nil, err, ""), nil, nil
		}
		id := submitted.ID
		if id == "" {
			return clientToolResult(response, nil, ""), nil, nil
		}
		response, err = c.RPC(ctx, WaitArgs{TaskID: id, WaitMS: args.WaitMS})
		if err != nil {
			return clientToolResult(nil, err, id), nil, nil
		}
		return clientRunResult(response, id), nil, nil
	})
	return server
}

func clientRunResult(data json.RawMessage, taskID string) *mcp.CallToolResult {
	if !json.Valid(data) {
		return clientToolResult(nil, errors.New("broker returned invalid task JSON"), taskID)
	}
	// Waiting is an open JSON forwarding boundary. Only replace the submitted
	// identity; keep unknown fields, absent fields and exact user numbers intact.
	var object map[string]json.RawMessage
	if json.Unmarshal(data, &object) == nil && object != nil {
		object["task_id"] = model.JSON(taskID)
		return clientToolResult(object, nil, taskID)
	}
	return clientToolResult(struct {
		TaskID string          `json:"task_id"`
		Result json.RawMessage `json:"result"`
	}{TaskID: taskID, Result: data}, nil, taskID)
}

func addClientTool[In clientTaskRequest](server *mcp.Server, c *Client, description string, validate func(In) error) {
	var input In
	name := input.Method()
	mcp.AddTool(server, &mcp.Tool{Name: name, Description: description}, func(ctx context.Context, req *mcp.CallToolRequest, args In) (*mcp.CallToolResult, any, error) {
		id := args.TaskIdentifier()
		if err := validate(args); err != nil {
			return clientToolResult(nil, err, id), nil, nil
		}
		// Per-call bounds prevent a broken broker from retaining unlimited waiters.
		ctx, cancel := context.WithTimeout(ctx, 70*time.Second)
		defer cancel()
		if name == "tasks_wait" {
			defer clientCallProgress(ctx, req)()
		}
		data, err := c.RPC(ctx, args)
		if err != nil {
			return clientToolResult(nil, err, id), nil, nil
		}
		if !json.Valid(data) {
			return clientToolResult(nil, errors.New("broker returned invalid task JSON"), id), nil, nil
		}
		// This tool only forwards the broker result. Preserve its JSON instead
		// of decoding unknown fields or user numbers through interface values.
		return clientToolResult(data, nil, id), nil, nil
	})
}

func clientJSONTaskID(data json.RawMessage) string {
	return mcputil.JSONTaskID(data)
}

func clientToolResult(value any, err error, taskID string) *mcp.CallToolResult {
	data, marshalErr := json.Marshal(value)
	if taskID == "" && marshalErr == nil {
		taskID = clientJSONTaskID(data)
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
		data, marshalErr = json.Marshal(clientErrorValue(domainError, taskID))
	}
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

func clientErrorValue(err *model.Error, taskID string) model.ErrorResult {
	return model.ErrorResult{Error: err, TaskID: taskID}
}

func clientSmallError(code, message, taskID string) *mcp.CallToolResult {
	return mcputil.SmallError(code, message, taskID)
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
