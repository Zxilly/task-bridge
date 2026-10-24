package model

import "context"

// ChannelNotification carries a task offer or recovery notice to Claude Code.
type ChannelNotification struct {
	Content string          `json:"content"`
	Meta    ChannelMetadata `json:"meta"`
}

// ChannelMetadata contains task routing identifiers. This is the channel's
// "meta" field, distinct from the standard MCP "_meta" field.
type ChannelMetadata struct {
	ResumeRequestID string `json:"resume_request_id,omitempty"`
	TaskID          string `json:"task_id"`
	AssignmentID    string `json:"assignment_id"`
	WorkspaceID     string `json:"workspace_id"`
}

// ChannelNotifier sends a typed notification; success does not acknowledge
// that Claude has accepted or processed the offered task.
type ChannelNotifier func(context.Context, ChannelNotification) error
