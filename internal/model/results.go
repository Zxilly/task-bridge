package model

// ErrorResult is the shared error body used by tools and the admin transport.
type ErrorResult struct {
	Error  *Error `json:"error"`
	TaskID string `json:"task_id,omitempty"`
}

type TaskSnapshot struct {
	Task              Task      `json:"task"`
	Commands          []Command `json:"commands"`
	CommandsTruncated bool      `json:"commands_truncated"`
}

type TaskWaitResult struct {
	Pending  bool         `json:"pending"`
	TaskID   string       `json:"task_id"`
	Snapshot TaskSnapshot `json:"snapshot"`
}

type TaskListResult struct {
	Tasks  []Task `json:"tasks"`
	Cursor int64  `json:"cursor"`
}

type CommandAuthorizeArgs struct {
	CommandID    string `json:"command_id"`
	AssignmentID string `json:"assignment_id"`
}

type CommandAuthorization struct {
	Allowed bool `json:"allowed"`
}

// OutputChunk preserves raw bytes and the byte cursor within the full output.
type OutputChunk struct {
	Seq    int64  `json:"chunk_seq"`
	Stream string `json:"stream"`
	Offset int64  `json:"offset"`
	Data   []byte `json:"data"`
}

type OutputResult struct {
	CommandID    string        `json:"command_id"`
	Stdout       string        `json:"stdout"`
	Stderr       string        `json:"stderr"`
	Chunks       []OutputChunk `json:"chunks"`
	TextOmitted  bool          `json:"text_omitted"`
	Cursor       int64         `json:"cursor"`
	HasMore      bool          `json:"has_more"`
	Truncated    bool          `json:"truncated"`
	DroppedBytes int64         `json:"dropped_bytes"`
}

// CommandResult extends the output page at the same JSON object level.
type CommandResult struct {
	OutputResult
	Command Command `json:"command"`
	Pending bool    `json:"pending"`
}

type ExecutorReconcileRecord struct {
	Error        *Error `json:"error,omitempty"`
	CommandID    string `json:"command_id"`
	TaskID       string `json:"task_id"`
	WorkspaceID  string `json:"workspace_id"`
	State        string `json:"state"`
	Command      string `json:"command"`
	OperationKey string `json:"operation_key"`
	Cwd          string `json:"cwd,omitempty"`
	TimeoutMS    int64  `json:"timeout_ms"`
	ExitCode     *int   `json:"exit_code,omitempty"`
	Signal       string `json:"signal,omitempty"`
	StartedAt    int64  `json:"started_at,omitempty"`
	FinishedAt   int64  `json:"finished_at,omitempty"`
	DurationMS   int64  `json:"duration_ms,omitempty"`
}

type ExecutorReconcileArgs struct {
	Reason  string                    `json:"reason"`
	Records []ExecutorReconcileRecord `json:"records"`
}

type ExecutorReconcileResult struct {
	KnownCommandIDs  []string                `json:"known_command_ids"`
	OrphanCommandIDs []string                `json:"orphan_command_ids"`
	ServerResults    map[string]CommandEvent `json:"server_results"`
}

type DeviceCreateResult struct {
	Device Principal `json:"device"`
	Token  string    `json:"token"`
}

type RevocationResult struct {
	Revoked bool `json:"revoked"`
}
type BackupResult struct {
	Output string `json:"output"`
}
type JournalResetResult struct {
	Reset                    bool `json:"reset"`
	OldExecutorTokensRevoked bool `json:"old_executor_tokens_revoked"`
}

// Event payload types preserve the fields present for each event variant.
type TaskStatePayload struct {
	State string `json:"state"`
}
type AssignmentPayload struct {
	AssignmentID string `json:"assignment_id"`
}
type CommandIDPayload struct {
	CommandID string `json:"command_id"`
}
type TaskProgressPayload struct {
	Message string `json:"message"`
}
type TaskCompletedPayload struct {
	State   string `json:"state"`
	Summary string `json:"summary"`
}
type TaskCancelRequestedPayload struct {
	Reason string `json:"reason"`
	State  string `json:"state"`
}
type CommandStatePayload struct {
	CommandID string `json:"command_id"`
	State     string `json:"state"`
}
