package agent

import (
	"context"
	"encoding/json"
	"io"
	"strings"
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
	if len(q.ready) != 2 {
		t.Fatalf("queued offers=%d", len(q.ready))
	}
	q.start(<-q.ready)
	q.start(<-q.ready)
	q.push(first)
	q.push(second)
	if len(q.ready) != 0 {
		t.Fatal("an active task was queued twice")
	}
	q.finish(first)
	q.push(first)
	if len(q.ready) != 0 {
		t.Fatal("completed task was queued again")
	}
	// A resume notification for one task must not suppress another task.
	resumed := first
	resumed.Meta.ResumeRequestID = "resume-first"
	q.push(resumed)
	if len(q.ready) != 1 {
		t.Fatal("resume notification was lost")
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
		{Method: "notifications/claude/channel", Params: model.JSON(model.ChannelNotification{Content: "text", Meta: model.ChannelMetadata{TaskID: "task", WorkspaceID: "workspace"}})},
		{Method: "notifications/claude/channel", Params: model.JSON(model.ChannelNotification{Content: "text", Meta: model.ChannelMetadata{TaskID: "task", WorkspaceID: "workspace", ResumeRequestID: strings.Repeat("r", 129)}})},
	} {
		connection := &channelConnection{Connection: &messageConnection{messages: []jsonrpc.Message{request}}, queue: newOfferQueue()}
		if _, err := connection.Read(context.Background()); err == nil {
			t.Fatal("invalid channel frame accepted")
		}
	}
}

func TestChannelResumeNoticeQueuesAfterInterruptedNotice(t *testing.T) {
	q := newOfferQueue()
	previous := model.ChannelNotification{Content: "interrupted", Meta: model.ChannelMetadata{TaskID: "task", AssignmentID: "old-assignment", WorkspaceID: "workspace"}}
	q.push(previous)
	q.start(<-q.ready)
	q.finish(previous)
	resume := model.ChannelNotification{Content: "resume", Meta: model.ChannelMetadata{TaskID: "task", WorkspaceID: "workspace", ResumeRequestID: "request"}}
	marker := &jsonrpc.Response{Result: json.RawMessage(`{}`)}
	connection := &channelConnection{Connection: &messageConnection{messages: []jsonrpc.Message{
		&jsonrpc.Request{Method: "notifications/claude/channel", Params: model.JSON(resume)},
		&jsonrpc.Request{Method: "notifications/claude/channel", Params: model.JSON(resume)},
		marker,
	}}, queue: q}
	if got, err := connection.Read(context.Background()); err != nil || got != marker {
		t.Fatalf("resume notice blocked reply: %v %v", got, err)
	}
	if len(q.ready) != 1 {
		t.Fatalf("queued resume notices=%d", len(q.ready))
	}
	if got := <-q.ready; got != resume {
		t.Fatalf("resume routing changed: %+v", got)
	}
}
