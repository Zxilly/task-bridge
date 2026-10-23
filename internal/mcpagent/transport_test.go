package mcpagent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/coder/websocket"
)

// Inspect the wire with independent JSON objects, so changes to the production
// struct tags or encoder cannot silently change the channel's public contract.
func receiveChannelNotification(t *testing.T, conn *websocket.Conn, want model.ChannelNotification) json.RawMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	typ, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("channel notification frame type=%v", typ)
	}
	var envelope map[string]json.RawMessage
	if err = json.Unmarshal(data, &envelope); err != nil {
		t.Fatal(err)
	}
	if _, present := envelope["id"]; present {
		t.Fatal("channel notification contains an id, including a possible null id")
	}
	if len(envelope) != 3 {
		t.Fatalf("channel envelope keys=%v", envelope)
	}
	for key, expected := range map[string]string{"jsonrpc": "2.0", "method": "notifications/claude/channel"} {
		var value string
		if err = json.Unmarshal(envelope[key], &value); err != nil || value != expected {
			t.Fatalf("envelope %s=%q err=%v, want %q", key, value, err, expected)
		}
	}
	var params map[string]json.RawMessage
	if err = json.Unmarshal(envelope["params"], &params); err != nil {
		t.Fatal(err)
	}
	if _, present := params["_meta"]; present {
		t.Fatal("channel metadata was encoded as standard MCP _meta")
	}
	if len(params) != 2 {
		t.Fatalf("channel params keys=%v", params)
	}
	var content string
	if err = json.Unmarshal(params["content"], &content); err != nil || content != want.Content {
		t.Fatalf("channel content mismatch: got bytes=%d want bytes=%d err=%v", len(content), len(want.Content), err)
	}
	var meta map[string]json.RawMessage
	if err = json.Unmarshal(params["meta"], &meta); err != nil {
		t.Fatal(err)
	}
	if len(meta) != 3 {
		t.Fatalf("channel metadata keys=%v", meta)
	}
	for key, expected := range map[string]string{"task_id": want.Meta.TaskID, "assignment_id": want.Meta.AssignmentID, "workspace_id": want.Meta.WorkspaceID} {
		var value *string
		if err = json.Unmarshal(meta[key], &value); err != nil || value == nil {
			t.Fatalf("channel meta.%s must be a JSON string, err=%v", key, err)
		}
		if *value != expected {
			t.Fatalf("channel meta.%s=%q, want %q", key, *value, expected)
		}
	}
	return envelope["params"]
}

func channelTransportPair(t *testing.T) (*wsConnection, *websocket.Conn) {
	t.Helper()
	type accepted struct {
		socket *websocket.Conn
		err    error
	}
	connections := make(chan accepted, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		socket, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"mcp"}})
		connections <- accepted{socket, err}
	}))
	t.Cleanup(server.Close)
	reader := dial(t, server)
	// The limit applies to params, so an exact-limit notification is larger on
	// the wire once its JSON-RPC envelope is included.
	reader.SetReadLimit(model.MaxMessageBytes)
	select {
	case result := <-connections:
		if result.err != nil {
			t.Fatal(result.err)
		}
		writer := &wsConnection{socket: result.socket, id: "channel-wire-test"}
		t.Cleanup(func() { _ = writer.Close() })
		return writer, reader
	case <-time.After(5 * time.Second):
		t.Fatal("WebSocket connection was not accepted")
		return nil, nil
	}
}

func TestChannelNotificationWireAndSerializedSize(t *testing.T) {
	meta := model.ChannelMetadata{TaskID: "task-1", AssignmentID: "assignment-1", WorkspaceID: "workspace-1"}
	// Calculate expected JSON sizes from the documented wire keys rather than
	// marshaling the production type, which could conceal a struct-tag mistake.
	const emptyWire = `{"content":"","meta":{"task_id":"task-1","assignment_id":"assignment-1","workspace_id":"workspace-1"}}`
	capacity := model.MaxToolBytes - len(emptyWire)
	ascii := strings.Repeat("x", capacity)
	utf8Content := strings.Repeat("界", capacity/3) + strings.Repeat("x", capacity%3)
	// NUL and each HTML character occupy six JSON bytes; quote, backslash, and
	// newline occupy two each, for thirty encoded bytes per seven-byte unit.
	escaped := strings.Repeat("\x00<>&\"\\\n", capacity/30) + strings.Repeat("x", capacity%30)
	for _, test := range []struct {
		name      string
		message   model.ChannelNotification
		wantBytes int
		wantError string
	}{
		{name: "empty_content", message: model.ChannelNotification{Meta: meta}, wantError: "non-empty"},
		{name: "empty_metadata", message: model.ChannelNotification{Content: "metadata strings may be empty"}},
		{name: "unicode_and_escaping", message: model.ChannelNotification{Content: "任务🙂\n\"quoted\"\\path <>&", Meta: meta}},
		{name: "ascii_exact_limit", message: model.ChannelNotification{Content: ascii, Meta: meta}, wantBytes: model.MaxToolBytes},
		{name: "ascii_one_byte_over", message: model.ChannelNotification{Content: ascii + "x", Meta: meta}, wantError: "exceeds limit"},
		{name: "utf8_exact_limit", message: model.ChannelNotification{Content: utf8Content, Meta: meta}, wantBytes: model.MaxToolBytes},
		{name: "utf8_one_byte_over", message: model.ChannelNotification{Content: utf8Content + "x", Meta: meta}, wantError: "exceeds limit"},
		{name: "json_escaping_exact_limit", message: model.ChannelNotification{Content: escaped, Meta: meta}, wantBytes: model.MaxToolBytes},
		{name: "json_escaping_one_byte_over", message: model.ChannelNotification{Content: escaped + "x", Meta: meta}, wantError: "exceeds limit"},
		{name: "metadata_counts_toward_limit", message: model.ChannelNotification{Content: "small content", Meta: model.ChannelMetadata{WorkspaceID: strings.Repeat("w", model.MaxToolBytes)}}, wantError: "exceeds limit"},
	} {
		t.Run(test.name, func(t *testing.T) {
			writer, reader := channelTransportPair(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := writer.notifyChannel(ctx, test.message)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("notification error=%v, want %q", err, test.wantError)
				}
				// Reading the next valid notification also proves rejection wrote
				// no partial or extra frame and did not poison the live connection.
				next := model.ChannelNotification{Content: "valid after rejected " + test.name, Meta: meta}
				if err = writer.notifyChannel(ctx, next); err != nil {
					t.Fatalf("valid notification after rejection failed: %v", err)
				}
				receiveChannelNotification(t, reader, next)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			params := receiveChannelNotification(t, reader, test.message)
			if test.wantBytes != 0 && len(params) != test.wantBytes {
				t.Fatalf("encoded params bytes=%d, want %d", len(params), test.wantBytes)
			}
		})
	}
}
