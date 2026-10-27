package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sort"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
)

func decodeUsageArgs(raw json.RawMessage, out any) error {
	if len(raw) > 8192 || len(bytes.TrimSpace(raw)) == 0 || bytes.TrimSpace(raw)[0] != '{' {
		return model.Err("invalid_arguments", "bounded usage argument object required")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return model.Err("invalid_arguments", "invalid usage arguments")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return model.Err("invalid_arguments", "one JSON value required")
	}
	return nil
}

// Account observations are transient and scoped to the authenticated A
// connection. A reconnect starts unknown; old connections cannot refresh it.
func (b *Broker) updateAgentUsage(ctx context.Context, p model.Principal, cid string, raw json.RawMessage) (model.AgentUsageAccepted, error) {
	var args model.AgentUsageUpdateArgs
	if err := decodeUsageArgs(raw, &args); err != nil {
		return model.AgentUsageAccepted{}, err
	}
	now := time.Now()
	if err := args.Usage.Validate(now); err != nil {
		return model.AgentUsageAccepted{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.Check(ctx, p); err != nil {
		return model.AgentUsageAccepted{}, err
	}
	slot := b.agents[p.AgentID]
	if slot == nil || slot.id != cid || slot.p.ID != p.ID {
		return model.AgentUsageAccepted{}, model.Err("stale_connection", "current agent connection required")
	}
	slot.usage = args.Usage.WithFreshness(now)
	slot.usageReceivedAt = now.Unix()
	return model.AgentUsageAccepted{Accepted: true}, nil
}

func (b *Broker) agentsUsage(ctx context.Context, peer *ClientPeer, raw json.RawMessage) (model.AgentsUsageResult, error) {
	var args model.AgentsUsageArgs
	if err := decodeUsageArgs(raw, &args); err != nil {
		return model.AgentsUsageResult{}, err
	}
	if !bounded(args.WorkspaceID, 128) || len(args.AgentID) > 128 {
		return model.AgentsUsageResult{}, model.Err("invalid_arguments", "workspace_id and optional agent_id must be bounded identifiers")
	}
	out := model.AgentsUsageResult{WorkspaceID: args.WorkspaceID, Agents: []model.AgentUsage{}}
	p := peer.Principal
	b.mu.Lock()
	defer b.mu.Unlock()
	err := b.Store.View(ctx, func(tx *storage.Tx) error {
		if err := activePrincipal(tx, p); err != nil {
			return err
		}
		if p.Kind != "executor" {
			return model.Err("forbidden", "executor identity required")
		}
		if !b.liveClient(peer) {
			return model.Err("stale_connection", "executor connection replaced")
		}
		workspace, err := tx.Workspace(args.WorkspaceID)
		if err != nil {
			return err
		}
		if workspace.ExecutorID != p.ExecutorID || (p.WorkspaceID != "" && p.WorkspaceID != workspace.ID) || !supportsWorkspace(peer, workspace.ID) {
			return model.Err("forbidden", "workspace outside executor scope")
		}
		for id, slot := range b.agents {
			if slot.p.WorkspaceID != workspace.ID || (args.AgentID != "" && id != args.AgentID) || activePrincipal(tx, slot.p) != nil {
				continue
			}
			if len(out.Agents) == 10 {
				return model.Err("result_too_large", "more than 10 connected agents; query with agent_id")
			}
			out.Agents = append(out.Agents, model.AgentUsage{AgentID: id, ReceivedAt: slot.usageReceivedAt, Usage: slot.usage.WithFreshness(time.Now())})
		}
		return nil
	})
	if err != nil {
		return model.AgentsUsageResult{}, normalize(err)
	}
	sort.Slice(out.Agents, func(i, j int) bool { return out.Agents[i].AgentID < out.Agents[j].AgentID })
	return out, nil
}
