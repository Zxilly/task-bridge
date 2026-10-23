package mcpagent

import (
	"context"
	"encoding/json"
	"math"
	"sync/atomic"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type toolDefinition struct {
	name        string
	description string
	required    []string
	properties  map[string]any
	readOnly    bool
}

func textSchema(max int) map[string]any {
	return map[string]any{"type": "string", "minLength": 1, "maxLength": max}
}
func integerSchema(min, max int) map[string]any {
	return map[string]any{"type": "integer", "minimum": min, "maximum": max}
}

func toolDefinitions() []toolDefinition {
	id := textSchema(128)
	assignment := map[string]any{"task_id": id, "assignment_id": id}
	command := func() map[string]any { return map[string]any{"task_id": id, "command_id": id} }
	get := command()
	wait := command()
	wait["wait_ms"] = integerSchema(0, 120000)
	output := command()
	output["cursor"] = integerSchema(0, 1<<53-1)
	output["limit"] = integerSchema(1, model.MaxOutputPage)
	cancel := command()
	cancel["reason"] = textSchema(1024)
	return []toolDefinition{
		{"task_accept", "Accept a channel task offer using its exact task_id and assignment_id.", []string{"task_id", "assignment_id"}, assignment, false},
		{"task_get", "Read the assigned task and command snapshot.", []string{"task_id"}, map[string]any{"task_id": id}, true},
		{"task_resume", "Request a new assignment after reconnect; unresolved commands require reconciliation first.", []string{"task_id", "previous_assignment_id"}, map[string]any{"task_id": id, "previous_assignment_id": id}, false},
		{"bash", "Execute Bash on the assigned B workspace. Reuse operation_key only for retries of the identical command. wait_ms bounds this call, not command execution.", []string{"task_id", "assignment_id", "operation_key", "command"}, map[string]any{"task_id": id, "assignment_id": id, "operation_key": textSchema(200), "command": textSchema(32768), "cwd": textSchema(4096), "timeout_ms": integerSchema(1, 900000), "wait_ms": integerSchema(0, 120000)}, false},
		{"command_get", "Get command status and execution metadata.", []string{"task_id", "command_id"}, get, true},
		{"command_wait", "Wait for a command with a bounded wait. Cancelling this MCP call does not cancel the command.", []string{"task_id", "command_id"}, wait, true},
		{"command_output", "Read a bounded output page. Continue with the returned cursor.", []string{"task_id", "command_id"}, output, true},
		{"command_cancel", "Explicitly request cancellation of a task command, including one from an older assignment.", []string{"task_id", "command_id"}, cancel, false},
		{"task_progress", "Record a concise task progress update for B (up to 2048 UTF-8 bytes).", []string{"task_id", "assignment_id", "message"}, map[string]any{"task_id": id, "assignment_id": id, "message": textSchema(2048)}, false},
		{"task_complete", "Complete the task only after all commands have known terminal outcomes. Summary is at most 2048 UTF-8 bytes; optional result is at most 4096 JSON bytes.", []string{"task_id", "assignment_id", "outcome", "summary"}, map[string]any{"task_id": id, "assignment_id": id, "outcome": map[string]any{"type": "string", "enum": []string{"succeeded", "failed"}}, "summary": textSchema(2048), "result": map[string]any{}}, false},
	}
}

func registerTools(server *mcp.Server, service model.AgentService, principal model.Principal, connectionID string, initialized *atomic.Bool) {
	// Waiters are request-scoped and bounded independently of durable work.
	// Reject excess calls immediately rather than allocating another waiter.
	slots := make(chan struct{}, 64)
	for _, def := range toolDefinitions() {
		schema := map[string]any{"type": "object", "additionalProperties": false, "required": def.required, "properties": def.properties}
		// Use the same validator as the official SDK, with a low-level handler
		// so validation failures also pass through the total response size cap.
		var parsed jsonschema.Schema
		_ = json.Unmarshal(model.JSON(schema), &parsed)
		resolved, err := parsed.Resolve(nil)
		if err != nil {
			panic(err)
		} // Static programmer-owned schemas.
		server.AddTool(&mcp.Tool{Name: def.name, Description: def.description, InputSchema: schema, Annotations: &mcp.ToolAnnotations{ReadOnlyHint: def.readOnly}}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			if !initialized.Load() {
				return toolResult(nil, model.Err("not_initialized", "MCP initialization is required")), nil
			}
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			default:
				return toolResult(nil, &model.Error{Code: "too_many_requests", Message: "at most 64 concurrent tool calls per connection", Retryable: true}), nil
			}
			var args any
			if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
				return toolResult(nil, model.Err("invalid_arguments", "arguments must be a JSON object")), nil
			}
			if err := resolved.Validate(args); err != nil {
				return toolResult(nil, model.Err("invalid_arguments", err.Error())), nil
			}
			finishProgress := beginWaitProgress(ctx, req, def.name)
			defer finishProgress()
			value, err := service.CallAgent(ctx, principal, connectionID, def.name, req.Params.Arguments)
			return toolResult(value, err), nil
		})
	}
}

func beginWaitProgress(ctx context.Context, req *mcp.CallToolRequest, method string) func() {
	noop := func() {}
	if method != "bash" && method != "command_wait" {
		return noop
	}
	token := req.Params.GetProgressToken()
	if !validProgressToken(token) {
		return noop
	}
	notify := func(progress float64, message string) {
		if ctx.Err() != nil {
			return
		}
		notifyCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		// Delivery failure must not turn a committed business operation into
		// a reported execution failure. This is advisory request progress.
		_ = req.Session.NotifyProgress(notifyCtx, &mcp.ProgressNotificationParams{ProgressToken: token, Progress: progress, Total: 1, Message: message})
	}
	notify(0, "Starting this tool call's bounded wait.")
	return func() { notify(1, "This tool call's wait has ended. Check the result for command and task state.") }
}

func validProgressToken(token any) bool {
	switch value := token.(type) {
	case string:
		return true
	case float64:
		return !math.IsInf(value, 0) && !math.IsNaN(value)
	case json.Number:
		number, err := value.Float64()
		return err == nil && !math.IsInf(number, 0) && !math.IsNaN(number)
	default:
		return false
	}
}

func toolResult(value any, err error) *mcp.CallToolResult {
	isError := err != nil
	if err != nil {
		value = map[string]any{"error": model.AsError(err)}
	}
	data, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		return smallError("internal", "tool result could not be serialized")
	}
	result := &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(data)}}, IsError: isError}
	// Measure the complete result, including JSON string escaping and MCP
	// wrappers. Do not duplicate data in StructuredContent.
	wire, marshalErr := json.Marshal(result)
	if marshalErr != nil || len(wire) > model.MaxToolBytes {
		return smallError("result_too_large", "result exceeds 32 KiB; request a smaller output page or summary")
	}
	return result
}

func smallError(code, message string) *mcp.CallToolResult {
	data := model.JSON(map[string]any{"error": &model.Error{Code: code, Message: message}})
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(data)}}, IsError: true}
}
