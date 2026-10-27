package agent

import (
	"bytes"
	"encoding/json"
	"math"
	"sync"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
)

// Snapshots belong to one invocation, including on resume. Never reuse another
// session's account observations or persist account data with broker task state.
type accountUsage struct {
	mu         sync.Mutex
	status     string
	observedAt int64
	windows    map[string]usageWindow
}

type usageWindow = model.UsageWindow
type usageSnapshot = model.AccountUsage

type rateWindow struct {
	Utilization *float64 `json:"utilization"`
	ResetsAt    *int64   `json:"resetsAt"`
}

func (u *accountUsage) observe(line []byte, sessionID string, now time.Time) {
	if u == nil {
		return
	}
	var event struct {
		Type      string `json:"type"`
		SessionID string `json:"session_id"`
		Info      struct {
			Status string `json:"status"`
			Type   string `json:"rateLimitType"`
			rateWindow
			// Observed in CLI output, but not a documented SDK guarantee.
			Windows map[string]rateWindow `json:"unifiedWindows"`
		} `json:"rate_limit_info"`
	}
	if json.Unmarshal(line, &event) != nil || event.Type != "rate_limit_event" || event.SessionID != sessionID {
		return
	}
	switch event.Info.Status {
	case "allowed", "allowed_warning", "rejected":
	default:
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.status = event.Info.Status
	u.observedAt = now.Unix()
	if u.windows == nil {
		u.windows = make(map[string]usageWindow)
	}
	update := func(name string, value rateWindow) {
		if !model.UsageWindowName(name) || (value.Utilization == nil && value.ResetsAt == nil) {
			return
		}
		if value.Utilization != nil && (math.IsNaN(*value.Utilization) || math.IsInf(*value.Utilization, 0) || *value.Utilization < 0 || *value.Utilization > 1) {
			return
		}
		if value.ResetsAt != nil && *value.ResetsAt <= 0 {
			return
		}
		window := usageWindow{ResetsAt: value.ResetsAt, ObservedAt: now.Unix()}
		if value.Utilization != nil {
			percent := *value.Utilization * 100
			window.UsedPercentage = &percent
		}
		u.windows[name] = window
	}
	update(event.Info.Type, event.Info.rateWindow)
	for name, window := range event.Info.Windows {
		update(name, window)
	}
}

func (u *accountUsage) snapshot(now time.Time) usageSnapshot {
	u.mu.Lock()
	defer u.mu.Unlock()
	return (usageSnapshot{Status: u.status, ObservedAt: u.observedAt, Windows: u.windows}).WithFreshness(now)
}

// stdout is NDJSON; stderr is kept separate so diagnostics cannot masquerade as
// account events. An oversized line is drained and skipped without blocking the
// process or losing subsequent events. The diagnostic buffer remains bounded.
type usageStream struct {
	diagnostic *launchDiagnostic
	usage      *accountUsage
	sessionID  string
	line       []byte
	skipping   bool
}

const maxUsageEventBytes = 1 << 20

func (s *usageStream) finish() {
	if !s.skipping && len(s.line) > 0 {
		s.usage.observe(s.line, s.sessionID, time.Now())
	}
	s.line = nil
}

func (s *usageStream) Write(data []byte) (int, error) {
	n := len(data)
	_, _ = s.diagnostic.Write(data)
	for len(data) > 0 {
		index := bytes.IndexByte(data, '\n')
		part := data
		if index >= 0 {
			part = data[:index]
		}
		if !s.skipping {
			if len(s.line)+len(part) > maxUsageEventBytes {
				s.line = nil
				s.skipping = true
			} else {
				s.line = append(s.line, part...)
			}
		}
		if index < 0 {
			break
		}
		if !s.skipping {
			s.usage.observe(s.line, s.sessionID, time.Now())
		}
		s.line = s.line[:0]
		s.skipping = false
		data = data[index+1:]
	}
	return n, nil
}
