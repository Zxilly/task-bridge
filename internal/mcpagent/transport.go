package mcpagent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ProtocolVersion is deliberately pinned: Claude Code's channel extension does
// not currently register on the 2026 MCP protocol. This is a supported SDK
// option, not a rewritten initialize response.
const ProtocolVersion = "2025-11-25"
const channelMethod = "notifications/claude/channel"

type wsTransport struct{ conn *wsConnection }

// NewTransport adapts an already connected WebSocket for an official MCP
// client or server session. Each transport is used for exactly one session.
func NewTransport(socket *websocket.Conn) mcp.Transport {
	socket.SetReadLimit(model.MaxMessageBytes)
	return &wsTransport{conn: &wsConnection{socket: socket, id: uuid.NewString()}}
}

func (t *wsTransport) Connect(context.Context) (mcp.Connection, error) { return t.conn, nil }
func (t *wsTransport) SupportsProtocolVersion(version string) bool     { return version == ProtocolVersion }

// wsConnection is only a framing adapter. The official SDK owns JSON-RPC
// dispatch, initialization, tool calls, cancellation and ping handling.
type wsConnection struct {
	socket    *websocket.Conn
	id        string
	closeOnce sync.Once
	closeErr  error
}

func (c *wsConnection) Read(ctx context.Context) (jsonrpc.Message, error) {
	typ, data, err := c.socket.Read(ctx)
	if err != nil {
		if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
			return nil, io.EOF
		}
		return nil, err
	}
	if typ != websocket.MessageText {
		return nil, fmt.Errorf("MCP requires text WebSocket messages")
	}
	return jsonrpc.DecodeMessage(data)
}

func (c *wsConnection) Write(ctx context.Context, message jsonrpc.Message) error {
	data, err := jsonrpc.EncodeMessage(message)
	if err != nil {
		return err
	}
	if len(data) > model.MaxMessageBytes {
		return fmt.Errorf("MCP message exceeds limit")
	}
	// A bounded write also prevents a slow peer from retaining a business
	// operation forever. coder/websocket serializes concurrent writes.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return c.socket.Write(ctx, websocket.MessageText, data)
}

func (c *wsConnection) Close() error {
	c.closeOnce.Do(func() { c.closeErr = c.socket.CloseNow() })
	return c.closeErr
}
func (c *wsConnection) SessionID() string { return c.id }

func (c *wsConnection) notifyChannel(ctx context.Context, notification model.ChannelNotification) error {
	// The SDK exposes custom methods for client requests, but no public
	// server-side extension notification method. Use its message encoder on
	// the same connection for this one extension notification (no request ID).
	if notification.Content == "" {
		return fmt.Errorf("channel content must be a non-empty string")
	}
	data, err := json.Marshal(notification)
	if err != nil {
		return err
	}
	if len(data) > model.MaxToolBytes {
		return fmt.Errorf("channel notification exceeds limit")
	}
	return c.Write(ctx, &jsonrpc.Request{Method: channelMethod, Params: data})
}
