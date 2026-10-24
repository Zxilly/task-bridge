package client

import (
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestClientMCPResumeAdvertisesSameTaskRecoveryAndReceipt(t *testing.T) {
	session := boundsTestSession(t, (&Client{}).MCPServer())
	listed, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Tools) != 8 {
		t.Fatalf("B exposes %d tools, want 8", len(listed.Tools))
	}
	for _, tool := range listed.Tools {
		if tool.Name != "tasks_resume" {
			continue
		}
		for _, meaning := range []string{"same task", "original Claude conversation", "idempotency_key", "does not mean recovery succeeded or the task completed", "tasks_get", "tasks_wait"} {
			if !strings.Contains(tool.Description, meaning) {
				t.Errorf("resume description omits %q: %s", meaning, tool.Description)
			}
		}
		data, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatal(err)
		}
		var schema struct {
			Properties map[string]json.RawMessage `json:"properties"`
			Required   []string                   `json:"required"`
		}
		if err := json.Unmarshal(data, &schema); err != nil {
			t.Fatal(err)
		}
		if len(schema.Properties) != 2 || schema.Properties["task_id"] == nil || schema.Properties["idempotency_key"] == nil {
			t.Fatalf("resume arguments are not limited to the existing task and request key: %s", data)
		}
		required := make(map[string]bool)
		for _, key := range schema.Required {
			required[key] = true
		}
		if len(required) != 2 || !required["task_id"] || !required["idempotency_key"] {
			t.Fatalf("resume schema omits required identity: %s", data)
		}
		return
	}
	t.Fatal("B did not expose tasks_resume")
}

func TestClientMCPResumeForwardsTypedRequestAndReceipt(t *testing.T) {
	const taskID = "original-task"
	requests := make(chan ResumeArgs, 3)
	c, ctx := clientMCPTestPeer(t, func(rpc model.RPC) model.Envelope {
		if rpc.Method != "tasks_resume" {
			t.Errorf("unexpected recovery RPC %q", rpc.Method)
		}
		var args ResumeArgs
		if err := json.Unmarshal(rpc.Args, &args); err != nil {
			t.Error(err)
		}
		requests <- args
		receipt := model.TaskResumeRequest{ID: "receipt-" + args.IdempotencyKey, TaskID: args.TaskID, IdempotencyKey: args.IdempotencyKey, State: args.IdempotencyKey, AgentID: "original-agent"}
		if receipt.State == "failed" {
			receipt.Error = &model.Error{Code: "saved_session_missing", Message: "original Claude session is unavailable"}
		}
		return model.Envelope{Payload: model.JSON(model.TaskResumeResult{TaskID: args.TaskID, ResumeRequest: receipt})}
	})
	session := clientMCPTestSession(t, ctx, c, nil)
	for _, state := range []string{"pending", "accepted", "failed"} {
		args := ResumeArgs{TaskID: taskID, IdempotencyKey: state}
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "tasks_resume", Arguments: args})
		if err != nil || result.IsError {
			t.Fatalf("durable %s receipt was not returned: %+v, %v", state, result, err)
		}
		if sent := <-requests; sent != args {
			t.Fatalf("resume request changed in transit: got %+v, want %+v", sent, args)
		}
		value := clientMCPResultValue[model.TaskResumeResult](t, result)
		if value.TaskID != taskID || value.ResumeRequest.TaskID != taskID || value.ResumeRequest.ID != "receipt-"+state || value.ResumeRequest.State != state || value.ResumeRequest.IdempotencyKey != state {
			t.Fatalf("task or resume receipt changed: %+v", value)
		}
		if state == "failed" && (value.ResumeRequest.Error == nil || value.ResumeRequest.Error.Code != "saved_session_missing") {
			t.Fatalf("durable resume failure was lost: %+v", value.ResumeRequest)
		}
	}
}

func TestClientMCPResumeRejectsMissingRequiredFieldsBeforeRPC(t *testing.T) {
	var calls atomic.Int64
	c, ctx := clientMCPTestPeer(t, func(model.RPC) model.Envelope {
		calls.Add(1)
		return model.Envelope{Payload: json.RawMessage(`{}`)}
	})
	session := clientMCPTestSession(t, ctx, c, nil)
	for _, args := range []map[string]string{{"task_id": "original-task"}, {"idempotency_key": "resume-1"}} {
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "tasks_resume", Arguments: args})
		if err == nil && (result == nil || !result.IsError) {
			t.Fatalf("incomplete resume request was accepted: %+v", args)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("SDK forwarded a resume request missing required identity")
	}
}

func TestClientMCPResumePreservesDynamicJSONAndBoundsResults(t *testing.T) {
	const opaque = `{"task_id":"original-task","resume_request":{"request_id":"receipt-1","state":"pending","extension":{"sequence":18446744073709551615}},"future_counter":9007199254740993}`
	c, ctx := clientMCPTestPeer(t, func(rpc model.RPC) model.Envelope {
		var args ResumeArgs
		if err := json.Unmarshal(rpc.Args, &args); err != nil {
			t.Error(err)
		}
		switch args.IdempotencyKey {
		case "oversized":
			return model.Envelope{Payload: model.JSON(struct {
				TaskID    string `json:"task_id"`
				Extension string `json:"extension"`
			}{args.TaskID, strings.Repeat("\x00", 4700)})}
		case "conflict":
			return model.Envelope{Error: &model.Error{Code: "resume_pending", Message: "retry the existing recovery request", Retryable: true}}
		default:
			return model.Envelope{Payload: json.RawMessage(opaque)}
		}
	})
	session := clientMCPTestSession(t, ctx, c, nil)
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "tasks_resume", Arguments: ResumeArgs{TaskID: "original-task", IdempotencyKey: "opaque"}})
	if err != nil || result.IsError {
		t.Fatalf("opaque resume receipt failed: %+v, %v", result, err)
	}
	var value struct {
		ResumeRequest struct {
			Extension struct {
				Sequence json.Number `json:"sequence"`
			} `json:"extension"`
		} `json:"resume_request"`
		FutureCounter json.Number `json:"future_counter"`
	}
	if err := json.Unmarshal(clientMCPResultJSON(t, result), &value); err != nil {
		t.Fatal(err)
	}
	if value.ResumeRequest.Extension.Sequence != "18446744073709551615" || value.FutureCounter != "9007199254740993" {
		t.Fatalf("resume forwarding changed unknown fields or JSON numbers: %+v", value)
	}
	for _, test := range []struct {
		key, code string
	}{{"oversized", "result_too_large"}, {"conflict", "resume_pending"}} {
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "tasks_resume", Arguments: ResumeArgs{TaskID: "original-task", IdempotencyKey: test.key}})
		if err != nil {
			t.Fatal(err)
		}
		value := clientMCPResultValue[model.ErrorResult](t, result)
		if !result.IsError || value.TaskID != "original-task" || value.Error == nil || value.Error.Code != test.code {
			t.Fatalf("resume error lost its bound or task identity: %+v", value)
		}
		if test.key == "conflict" && !value.Error.Retryable {
			t.Fatal("resume domain error lost retryability")
		}
	}
}
