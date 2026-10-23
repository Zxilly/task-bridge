// Package model contains the shared, versioned wire and domain contracts.
package model

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const Version = "0.1.0-dev"
const ProtocolVersion = 1
const MaxMessageBytes = 1 << 20
const MaxToolBytes = 32 << 10
const MaxOutputPage = 16 << 10

type Error struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func (e *Error) Error() string       { return e.Code + ": " + e.Message }
func Err(code, message string) error { return &Error{Code: code, Message: message} }
func AsError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	if errors.Is(err, context.Canceled) {
		return &Error{Code: "call_cancelled", Message: "request was cancelled; durable work may still be running", Retryable: true}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Code: "call_timeout", Message: "request timed out; query the durable task or command before retrying", Retryable: true}
	}
	return &Error{Code: "internal", Message: "operation failed"}
}

type Principal struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	WorkspaceID string `json:"workspace_id,omitempty"`
	ExecutorID  string `json:"executor_id,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	TokenHash   string `json:"-"`
	Disabled    bool   `json:"disabled"`
}
type Workspace struct {
	ID               string `json:"id"`
	ExecutorID       string `json:"executor_id"`
	Description      string `json:"description,omitempty"`
	Quarantined      bool   `json:"quarantined"`
	QuarantineReason string `json:"quarantine_reason,omitempty"`
}
type Task struct {
	ID             string          `json:"task_id"`
	RequesterID    string          `json:"requester_id"`
	WorkspaceID    string          `json:"workspace_id"`
	ExecutorID     string          `json:"executor_id"`
	Prompt         string          `json:"prompt"`
	State          string          `json:"state"`
	IdempotencyKey string          `json:"idempotency_key"`
	RequestHash    string          `json:"-"`
	AssignmentID   string          `json:"assignment_id,omitempty"`
	AgentID        string          `json:"agent_id,omitempty"`
	ConnectionID   string          `json:"connection_id,omitempty"`
	LeaseEpoch     int64           `json:"lease_epoch"`
	LeaseUntil     int64           `json:"lease_until,omitempty"`
	DeadlineAt     int64           `json:"deadline_at"`
	CreatedAt      int64           `json:"created_at"`
	UpdatedAt      int64           `json:"updated_at"`
	Revision       int64           `json:"revision"`
	Summary        string          `json:"summary,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
	Progress       string          `json:"progress,omitempty"`
	CancelReason   string          `json:"cancel_reason,omitempty"`
	Unresolved     bool            `json:"unresolved,omitempty"`
}

func (t Task) Terminal() bool {
	switch t.State {
	case "succeeded", "failed", "cancelled", "timed_out", "abandoned":
		return true
	}
	return false
}

type Command struct {
	Error        *Error `json:"error,omitempty"`
	ID           string `json:"command_id"`
	TaskID       string `json:"task_id"`
	AssignmentID string `json:"assignment_id"`
	OperationKey string `json:"operation_key"`
	RequestHash  string `json:"-"`
	Command      string `json:"command"`
	Cwd          string `json:"cwd,omitempty"`
	RequestedCwd string `json:"requested_cwd,omitempty"`
	TimeoutMS    int64  `json:"timeout_ms"`
	State        string `json:"state"`
	ExitCode     *int   `json:"exit_code"`
	Signal       string `json:"signal,omitempty"`
	StartedAt    int64  `json:"started_at,omitempty"`
	FinishedAt   int64  `json:"finished_at,omitempty"`
	CreatedAt    int64  `json:"created_at"`
	DurationMS   int64  `json:"duration_ms"`
	ValidUntil   int64  `json:"valid_until"`
	OutputBytes  int64  `json:"output_bytes"`
	Truncated    bool   `json:"truncated"`
	DroppedBytes int64  `json:"dropped_bytes"`
	CancelReason string `json:"cancel_reason,omitempty"`
}

func (c Command) Terminal() bool {
	switch c.State {
	case "exited", "cancelled", "timed_out", "unknown", "not_started":
		return true
	}
	return false
}

type Chunk struct {
	CommandID string `json:"command_id"`
	Seq       int64  `json:"chunk_seq"`
	Stream    string `json:"stream"`
	Data      []byte `json:"data"`
}
type CommandEvent struct {
	Error        *Error `json:"error,omitempty"`
	CommandID    string `json:"command_id"`
	State        string `json:"state,omitempty"`
	ExitCode     *int   `json:"exit_code,omitempty"`
	Signal       string `json:"signal,omitempty"`
	Cwd          string `json:"cwd,omitempty"`
	StartedAt    int64  `json:"started_at,omitempty"`
	FinishedAt   int64  `json:"finished_at,omitempty"`
	DurationMS   int64  `json:"duration_ms,omitempty"`
	Truncated    bool   `json:"truncated,omitempty"`
	DroppedBytes int64  `json:"dropped_bytes,omitempty"`
	Chunk        *Chunk `json:"chunk,omitempty"`
}
type Dispatch struct {
	WorkspaceID string  `json:"workspace_id"`
	Command     Command `json:"command"`
}
type Event struct {
	Seq       int64           `json:"seq"`
	TaskID    string          `json:"task_id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	CreatedAt int64           `json:"created_at"`
}
type Envelope struct {
	Version     int             `json:"version"`
	Type        string          `json:"type"`
	ID          string          `json:"message_id,omitempty"`
	RequestID   string          `json:"request_id,omitempty"`
	ServerEpoch string          `json:"server_epoch,omitempty"`
	StreamID    string          `json:"stream_id,omitempty"`
	Seq         int64           `json:"seq,omitempty"`
	Generation  int64           `json:"connection_generation,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	Error       *Error          `json:"error,omitempty"`
}
type RPC struct {
	Method string          `json:"method"`
	Args   json.RawMessage `json:"args"`
}
type Hello struct {
	ExecutorID string   `json:"executor_id"`
	Version    string   `json:"client_version"`
	Workspaces []string `json:"workspaces"`
	StreamID   string   `json:"stream_id"`
}
type Welcome struct {
	ServerEpoch string `json:"server_epoch"`
	StreamID    string `json:"stream_id"`
	Generation  int64  `json:"connection_generation"`
}

func JSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("marshal contract: %v", err))
	}
	return b
}
func Now() int64 { return time.Now().UTC().UnixMilli() }

// AgentService keeps MCP transport independent of persistence and routing.
type AgentService interface {
	CallAgent(context.Context, Principal, string, string, json.RawMessage) (any, error)
	AgentConnected(context.Context, Principal, string, ChannelNotifier) error
	AgentDisconnected(Principal, string)
}
