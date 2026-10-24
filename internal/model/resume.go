package model

// TaskResumeRequest records one explicit B-side attempt to continue a task.
// Accepted means that one controller has consumed the request, not that its
// Claude process has started or that the task has completed.
type TaskResumeRequest struct {
	ID                   string `json:"request_id"`
	TaskID               string `json:"task_id"`
	RequesterID          string `json:"requester_id"`
	IdempotencyKey       string `json:"idempotency_key"`
	State                string `json:"state"`
	AgentID              string `json:"agent_id"`
	PreviousAssignmentID string `json:"previous_assignment_id,omitempty"`
	AssignmentID         string `json:"assignment_id,omitempty"`
	ConnectionID         string `json:"connection_id,omitempty"`
	CreatedAt            int64  `json:"created_at"`
	UpdatedAt            int64  `json:"updated_at"`
	AcceptedAt           int64  `json:"accepted_at,omitempty"`
	Error                *Error `json:"error,omitempty"`
}

type TaskResumeArgs struct {
	TaskID         string `json:"task_id" jsonschema:"Existing interrupted task originally submitted by this executor credential"`
	IdempotencyKey string `json:"idempotency_key" jsonschema:"Reuse this key when retrying the same resume request; a new key explicitly requests another attempt"`
}

type TaskResumeResult struct {
	TaskID        string            `json:"task_id"`
	ResumeRequest TaskResumeRequest `json:"resume_request"`
}

func (TaskResumeArgs) clientRequest()           {}
func (TaskResumeArgs) Method() string           { return "tasks_resume" }
func (a TaskResumeArgs) TaskIdentifier() string { return a.TaskID }
func (TaskResumeResult) clientResult()          {}
func (TaskResumeResult) agentResult()           {}
func (TaskResumeRequest) eventPayload()         {}
