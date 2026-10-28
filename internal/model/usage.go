package model

import (
	"math"
	"time"
)

const AccountUsageSource = "claude_code_rate_limit_event"
const AccountUsageCommandSource = "claude_code_usage_command"

type UsageWindow struct {
	UsedPercentage *float64 `json:"used_percentage,omitempty"`
	ResetsAt       *int64   `json:"resets_at,omitempty"`
	ObservedAt     int64    `json:"observed_at"`
	Stale          bool     `json:"stale"`
}

// Usage timestamps use Unix seconds, matching Claude Code's reset times.
type AccountUsage struct {
	Available  bool                   `json:"available"`
	Source     string                 `json:"source"`
	Status     string                 `json:"status,omitempty"`
	ObservedAt int64                  `json:"observed_at,omitempty"`
	Stale      bool                   `json:"stale"`
	Reason     string                 `json:"reason,omitempty"`
	Windows    map[string]UsageWindow `json:"windows"`
}

func UsageWindowName(name string) bool {
	switch name {
	case "five_hour", "seven_day", "seven_day_opus", "seven_day_sonnet":
		return true
	}
	return false
}

// WithFreshness returns an independent copy and derives display fields instead
// of trusting a reporter's availability, staleness or explanation text.
func (u AccountUsage) WithFreshness(now time.Time) AccountUsage {
	if u.Source == "" {
		u.Source = AccountUsageSource
	}
	u.Reason = ""
	u.Stale = u.ObservedAt == 0 || now.Unix()-u.ObservedAt > 300
	windows := make(map[string]UsageWindow, len(u.Windows))
	for name, window := range u.Windows {
		window.Stale = now.Unix()-window.ObservedAt > 300 || (window.ResetsAt != nil && now.Unix() >= *window.ResetsAt)
		u.Stale = u.Stale || window.Stale
		if window.UsedPercentage != nil {
			value := *window.UsedPercentage
			window.UsedPercentage = &value
		}
		if window.ResetsAt != nil {
			value := *window.ResetsAt
			window.ResetsAt = &value
		}
		windows[name] = window
	}
	u.Windows = windows
	u.Available = len(windows) > 0
	if !u.Available {
		u.Stale = true
		u.Reason = "No account window reported for this invocation yet; Claude Code may not report subscription limits for this login or version."
	}
	return u
}

func (u AccountUsage) Validate(now time.Time) error {
	if (u.Source != AccountUsageSource && u.Source != AccountUsageCommandSource) || u.ObservedAt < 0 || u.ObservedAt > now.Unix()+300 || len(u.Windows) > 4 {
		return Err("invalid_arguments", "invalid account usage source, timestamp or window count")
	}
	switch u.Status {
	case "", "allowed", "allowed_warning", "rejected":
	default:
		return Err("invalid_arguments", "invalid account usage status")
	}
	for name, window := range u.Windows {
		if !UsageWindowName(name) || window.ObservedAt <= 0 || window.ObservedAt > u.ObservedAt || (window.UsedPercentage == nil && window.ResetsAt == nil) {
			return Err("invalid_arguments", "invalid account usage window")
		}
		if p := window.UsedPercentage; p != nil && (math.IsNaN(*p) || math.IsInf(*p, 0) || *p < 0 || *p > 100) {
			return Err("invalid_arguments", "account usage percentage must be 0..100")
		}
		if window.ResetsAt != nil && *window.ResetsAt <= 0 {
			return Err("invalid_arguments", "invalid account usage reset time")
		}
	}
	return nil
}

type AgentUsageUpdateArgs struct {
	Usage AccountUsage `json:"usage"`
}

type AgentUsageAccepted struct {
	Accepted bool `json:"accepted"`
}

func (AgentUsageAccepted) agentResult() {}

type AgentsUsageArgs struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id,omitempty"`
}

func (AgentsUsageArgs) clientRequest()         {}
func (AgentsUsageArgs) Method() string         { return "agents_usage" }
func (AgentsUsageArgs) TaskIdentifier() string { return "" }

type AgentUsage struct {
	AgentID    string       `json:"agent_id"`
	ReceivedAt int64        `json:"received_at,omitempty"`
	Usage      AccountUsage `json:"usage"`
}

type AgentsUsageResult struct {
	WorkspaceID string       `json:"workspace_id"`
	Agents      []AgentUsage `json:"agents"`
}

func (AgentsUsageResult) clientResult() {}
