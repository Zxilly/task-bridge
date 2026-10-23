package agent

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// offerQueue never blocks the SDK reader: ordinary replies and pings must keep
// flowing while a Claude child is running. The broker retries unaccepted offers.
type offerQueue struct {
	mu                        sync.Mutex
	ready                     chan model.ChannelNotification
	active, queued, completed string
}

func newOfferQueue() *offerQueue                  { return &offerQueue{ready: make(chan model.ChannelNotification, 1)} }
func offerKey(n model.ChannelNotification) string { return n.Meta.TaskID + "/" + n.Meta.AssignmentID }
func (q *offerQueue) push(n model.ChannelNotification) {
	q.mu.Lock()
	defer q.mu.Unlock()
	key := offerKey(n)
	if key == q.active || key == q.queued || key == q.completed {
		return
	}
	select {
	case q.ready <- n:
		q.queued = key
	default:
	}
}
func (q *offerQueue) start(n model.ChannelNotification) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queued, q.active = "", offerKey(n)
}
func (q *offerQueue) finish(n model.ChannelNotification) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.active, q.completed = "", offerKey(n)
}

type channelTransport struct {
	mcp.Transport
	queue *offerQueue
}
type channelConnection struct {
	mcp.Connection
	queue *offerQueue
}

func (t channelTransport) Connect(ctx context.Context) (mcp.Connection, error) {
	conn, err := t.Transport.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &channelConnection{Connection: conn, queue: t.queue}, nil
}
func (c *channelConnection) Read(ctx context.Context) (jsonrpc.Message, error) {
	for {
		message, err := c.Connection.Read(ctx)
		if err != nil {
			return nil, err
		}
		request, ok := message.(*jsonrpc.Request)
		if !ok || request.Method != "notifications/claude/channel" {
			return message, nil
		}
		if request.IsCall() {
			return nil, errors.New("channel event must be a notification")
		}
		var offer model.ChannelNotification
		if len(request.Params) > model.MaxToolBytes || json.Unmarshal(request.Params, &offer) != nil || offer.Content == "" || offer.Meta.TaskID == "" || offer.Meta.AssignmentID == "" || offer.Meta.WorkspaceID == "" {
			return nil, errors.New("invalid task channel notification")
		}
		if len(offer.Meta.TaskID) > 128 || len(offer.Meta.AssignmentID) > 128 || len(offer.Meta.WorkspaceID) > 128 {
			return nil, errors.New("invalid task channel identity")
		}
		c.queue.push(offer)
	}
}
