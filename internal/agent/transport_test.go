package agent

import (
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

type messageConnection struct{ messages []jsonrpc.Message }

func (c *messageConnection) Read(context.Context) (jsonrpc.Message, error) {
	if len(c.messages) == 0 {
		return nil, io.EOF
	}
	message := c.messages[0]
	c.messages = c.messages[1:]
	return message, nil
}
func (*messageConnection) Write(context.Context, jsonrpc.Message) error { return nil }
func (*messageConnection) Close() error                                 { return nil }
func (*messageConnection) SessionID() string                            { return "test" }

func TestChannelQueueDoesNotBlockProtocolRepliesAndDeduplicates(t *testing.T) {
	q := newOfferQueue()
	first := model.ChannelNotification{Content: "first", Meta: model.ChannelMetadata{TaskID: "task1", AssignmentID: "assignment1", WorkspaceID: "workspace"}}
	second := model.ChannelNotification{Content: "second", Meta: model.ChannelMetadata{TaskID: "task2", AssignmentID: "assignment2", WorkspaceID: "workspace"}}
	marker := &jsonrpc.Response{Result: json.RawMessage(`{"ok":true}`)}
	connection := &channelConnection{Connection: &messageConnection{messages: []jsonrpc.Message{
		&jsonrpc.Request{Method: "notifications/claude/channel", Params: model.JSON(first)},
		&jsonrpc.Request{Method: "notifications/claude/channel", Params: model.JSON(first)},
		&jsonrpc.Request{Method: "notifications/claude/channel", Params: model.JSON(second)},
		marker,
	}}, queue: q}
	if got, err := connection.Read(context.Background()); err != nil || got != marker {
		t.Fatalf("reply was blocked or altered: %v %v", got, err)
	}
	if len(q.ready) != 1 {
		t.Fatalf("queued offers=%d", len(q.ready))
	}
	q.start(<-q.ready)
	q.push(first)
	if len(q.ready) != 0 {
		t.Fatal("active task was queued twice")
	}
	q.push(second)
	q.finish(first)
	if got := <-q.ready; got.Meta.TaskID != "task2" {
		t.Fatalf("next offer=%+v", got)
	}
	q.push(first)
	if len(q.ready) != 0 {
		t.Fatal("completed task was queued again")
	}
}

func TestChannelTransportRejectsCallsAndMalformedRouting(t *testing.T) {
	id, err := jsonrpc.MakeID("request")
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []*jsonrpc.Request{
		{ID: id, Method: "notifications/claude/channel", Params: json.RawMessage(`{}`)},
		{Method: "notifications/claude/channel", Params: json.RawMessage(`{"content":"text","meta":{}}`)},
		{Method: "notifications/claude/channel", Params: json.RawMessage(`null`)},
	} {
		connection := &channelConnection{Connection: &messageConnection{messages: []jsonrpc.Message{request}}, queue: newOfferQueue()}
		if _, err := connection.Read(context.Background()); err == nil {
			t.Fatal("invalid channel frame accepted")
		}
	}
}
