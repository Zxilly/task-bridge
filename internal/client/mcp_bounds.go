package client

import (
	"github.com/Zxilly/task-bridge/internal/mcputil"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func addClientResultBounds(server *mcp.Server, implementation *mcp.Implementation) {
	mcputil.AddResultBounds(server, implementation)
}
