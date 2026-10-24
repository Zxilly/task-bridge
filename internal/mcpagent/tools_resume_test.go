package mcpagent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestOfficialSDKResumeToolsValidateAndRoute(t *testing.T) {
	service := newFake()
	server := testServer(t, service)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	session, err := mcp.NewClient(&mcp.Implementation{Name: "resume-schema-test", Version: "1"}, nil).Connect(ctx, NewTransport(dial(t, server)), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	list, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range list.Tools {
		if tool.Name == "task_resume" {
			var schema struct {
				Required []string `json:"required"`
			}
			data, err := json.Marshal(tool.InputSchema)
			if err != nil || json.Unmarshal(data, &schema) != nil || len(schema.Required) != 1 || schema.Required[0] != "task_id" {
				t.Fatalf("resume schema incorrectly requires one recovery form: %s (%v)", data, err)
			}
		}
		if tool.Name == "task_resume_failed" && !strings.Contains(tool.Description, "Controller-only") {
			t.Fatal("recovery failure reporting does not identify its controller scope")
		}
	}
	for _, tc := range []struct {
		name, method string
		args         map[string]any
		valid        bool
	}{
		{"request claim", "task_resume", map[string]any{"task_id": "task", "resume_request_id": "request"}, true},
		{"manual recovery", "task_resume", map[string]any{"task_id": "task", "previous_assignment_id": "assignment"}, true},
		// The broker owns the cross-field requirement; the schema permits either form.
		{"broker validates recovery ID presence", "task_resume", map[string]any{"task_id": "task"}, true},
		{"bounded recovery IDs", "task_resume", map[string]any{"task_id": "task", "previous_assignment_id": strings.Repeat("a", 128), "resume_request_id": strings.Repeat("r", 128)}, true},
		{"missing task", "task_resume", map[string]any{"resume_request_id": "request"}, false},
		{"empty request", "task_resume", map[string]any{"task_id": "task", "resume_request_id": ""}, false},
		{"long request", "task_resume", map[string]any{"task_id": "task", "resume_request_id": strings.Repeat("r", 129)}, false},
		{"long previous assignment", "task_resume", map[string]any{"task_id": "task", "previous_assignment_id": strings.Repeat("a", 129)}, false},
		{"unknown claim field", "task_resume", map[string]any{"task_id": "task", "resume_request_id": "request", "force": true}, false},
		{"failure report", "task_resume_failed", map[string]any{"task_id": "task", "resume_request_id": "request", "error_code": "missing_session", "message": "Saved conversation is missing."}, true},
		{"diagnostic exact limits", "task_resume_failed", map[string]any{"task_id": "task", "resume_request_id": "request", "error_code": strings.Repeat("c", 64), "message": strings.Repeat("m", 1024)}, true},
		{"long error code", "task_resume_failed", map[string]any{"task_id": "task", "resume_request_id": "request", "error_code": strings.Repeat("c", 65), "message": "missing"}, false},
		{"long message", "task_resume_failed", map[string]any{"task_id": "task", "resume_request_id": "request", "error_code": "missing_session", "message": strings.Repeat("m", 1025)}, false},
		{"missing failure task", "task_resume_failed", map[string]any{"resume_request_id": "request", "error_code": "missing_session", "message": "missing"}, false},
		{"missing failure request", "task_resume_failed", map[string]any{"task_id": "task", "error_code": "missing_session", "message": "missing"}, false},
		{"missing failure code", "task_resume_failed", map[string]any{"task_id": "task", "resume_request_id": "request", "message": "missing"}, false},
		{"missing failure message", "task_resume_failed", map[string]any{"task_id": "task", "resume_request_id": "request", "error_code": "missing_session"}, false},
		{"empty failure message", "task_resume_failed", map[string]any{"task_id": "task", "resume_request_id": "request", "error_code": "missing_session", "message": ""}, false},
		{"unknown failure field", "task_resume_failed", map[string]any{"task_id": "task", "resume_request_id": "request", "error_code": "missing_session", "message": "missing", "assignment_id": "old"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tc.method, Arguments: tc.args})
			if err != nil {
				t.Fatal(err)
			}
			if result.IsError == tc.valid {
				t.Fatalf("unexpected schema result: %+v", result)
			}
			if tc.valid {
				select {
				case call := <-service.calls:
					if call.name != tc.method || call.principal.AgentID != "a" || call.connectionID == "" {
						t.Fatalf("recovery request lost authenticated routing: %+v", call)
					}
					var forwarded map[string]any
					if err := json.Unmarshal(call.args, &forwarded); err != nil || len(forwarded) != len(tc.args) {
						t.Fatalf("recovery request lost arguments: %s (%v)", call.args, err)
					}
					for key, want := range tc.args {
						if forwarded[key] != want {
							t.Fatalf("forwarded %s=%v, want %v", key, forwarded[key], want)
						}
					}
				default:
					t.Fatal("valid recovery request did not reach broker service")
				}
				return
			}
			select {
			case <-service.calls:
				t.Fatal("invalid recovery request reached broker service")
			default:
			}
			var body struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal([]byte(result.Content[0].(*mcp.TextContent).Text), &body); err != nil || body.Error.Code != "invalid_arguments" {
				t.Fatalf("schema rejection lost domain error: %+v (%v)", body, err)
			}
		})
	}
}
