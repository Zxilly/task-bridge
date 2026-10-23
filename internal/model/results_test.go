package model

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

// The old wire objects are deliberately independent of the new structs. These
// checks catch accidental nesting, omitted zero values, and null/array changes
// when replacing dynamic RPC results with shared contracts.
func TestRPCResultWireCompatibility(t *testing.T) {
	task := Task{ID: "task-1", State: "running", Result: json.RawMessage(`{"integer":9007199254740993}`)}
	command := Command{ID: "command-1", TaskID: task.ID, State: "running"}
	snapshot := TaskSnapshot{Task: task, Commands: []Command{}, CommandsTruncated: false}
	legacySnapshot := map[string]any{"task": task, "commands": []Command{}, "commands_truncated": false}
	chunk := OutputChunk{Seq: 2, Stream: "stdout", Offset: 1, Data: []byte{0xff, 0x00, 0xe4}}
	page := OutputResult{CommandID: command.ID, Chunks: []OutputChunk{chunk}, TextOmitted: true, Cursor: 4, HasMore: true}
	legacyPage := map[string]any{
		"command_id": command.ID, "stdout": "", "stderr": "",
		"chunks":       []map[string]any{{"chunk_seq": int64(2), "stream": "stdout", "offset": int64(1), "data": chunk.Data}},
		"text_omitted": true, "cursor": int64(4), "has_more": true, "truncated": false, "dropped_bytes": int64(0),
	}
	legacyCommand := map[string]any{}
	for key, value := range legacyPage {
		legacyCommand[key] = value
	}
	legacyCommand["command"] = command
	legacyCommand["pending"] = true
	domainError := &Error{Code: "call_timeout", Message: "query the durable task", Retryable: true}
	cases := []struct {
		name   string
		typed  any
		legacy any
	}{
		{"snapshot", snapshot, legacySnapshot},
		{"wait", TaskWaitResult{Pending: true, TaskID: task.ID, Snapshot: snapshot}, map[string]any{"pending": true, "task_id": task.ID, "snapshot": legacySnapshot}},
		{"empty_list", TaskListResult{Tasks: []Task{}}, map[string]any{"tasks": []Task{}, "cursor": int64(0)}},
		{"empty_events", EventList{}, []Event{}},
		{"nil_events", EventList(nil), []Event(nil)},
		{"empty_principals", PrincipalList{}, []Principal{}},
		{"nil_principals", PrincipalList(nil), []Principal(nil)},
		{"output_page", page, legacyPage},
		{"command_result_flattened", CommandResult{OutputResult: page, Command: command, Pending: true}, legacyCommand},
		{"empty_output", OutputResult{Chunks: []OutputChunk{}}, map[string]any{"command_id": "", "stdout": "", "stderr": "", "chunks": []any{}, "text_omitted": false, "cursor": int64(0), "has_more": false, "truncated": false, "dropped_bytes": int64(0)}},
		{"empty_reconciliation", ExecutorReconcileResult{KnownCommandIDs: []string{}, OrphanCommandIDs: []string{}, ServerResults: map[string]CommandEvent{}}, map[string]any{"known_command_ids": []string{}, "orphan_command_ids": []string{}, "server_results": map[string]any{}}},
		{"error_without_task", ErrorResult{Error: domainError}, map[string]any{"error": domainError}},
		{"error_with_task", ErrorResult{Error: domainError, TaskID: task.ID}, map[string]any{"error": domainError, "task_id": task.ID}},
		{"empty_completion_summary", TaskCompletedPayload{State: "failed"}, map[string]any{"state": "failed", "summary": ""}},
		{"empty_cancel_reason", TaskCancelRequestedPayload{State: "cancel_requested"}, map[string]any{"state": "cancel_requested", "reason": ""}},
		{"admin_result", AdminResponse[TaskSnapshot]{Result: snapshot}, map[string]any{"result": legacySnapshot}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			typed, legacy := JSON(tc.typed), JSON(tc.legacy)
			if !reflect.DeepEqual(decodeWireJSON(t, typed), decodeWireJSON(t, legacy)) {
				t.Fatalf("wire contract changed\ntyped:  %s\nlegacy: %s", typed, legacy)
			}
			// Field order may change; the existing complete-response byte limits
			// must still see the same encoded payload size.
			if len(typed) != len(legacy) {
				t.Fatalf("encoded payload size changed: typed=%d legacy=%d", len(typed), len(legacy))
			}
		})
	}
}

func decodeWireJSON(t *testing.T, raw []byte) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}
