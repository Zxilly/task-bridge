package agent

import (
	"bytes"
	"context"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Keep network requests off the Claude stdout reader. Coalesce observations and
// retry failed reports while the invocation is running.
func (w *Worker) reportUsage(ctx context.Context, upstream *mcp.ClientSession, usage *accountUsage) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var sent []byte
	for {
		if ctx.Err() != nil {
			return
		}
		snapshot := usage.snapshot(time.Now())
		data := model.JSON(snapshot)
		if !bytes.Equal(sent, data) && w.publishUsage(ctx, upstream, snapshot) {
			sent = data
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) publishUsage(ctx context.Context, upstream *mcp.ClientSession, usage model.AccountUsage) bool {
	if ctx.Err() != nil {
		return false
	}
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	result, err := upstream.CallTool(callCtx, &mcp.CallToolParams{Name: "agent_usage_update", Arguments: model.AgentUsageUpdateArgs{Usage: usage}})
	if err == nil {
		_, err = decodeTool[model.AgentUsageAccepted](result)
	}
	if err != nil && ctx.Err() == nil {
		w.log.Warn("could not report Claude account usage", "error", err)
	}
	return err == nil
}
