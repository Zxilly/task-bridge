package model

// ClientRequest couples each argument contract to its protocol method.
type ClientRequest interface {
	clientRequest()
	Method() string
}

type TaskSubmitArgs struct {
	WorkspaceID    string `json:"workspace_id" jsonschema:"Configured local workspace identifier"`
	Prompt         string `json:"prompt" jsonschema:"English only (ASCII). Use printable ASCII plus TAB, LF or CR and include at least one A-Z or a-z letter."`
	IdempotencyKey string `json:"idempotency_key" jsonschema:"Stable key reused if submission times out or disconnects"`
	DeadlineMS     int64  `json:"deadline_ms,omitempty"`
}

type TaskRunArgs struct {
	WorkspaceID    string `json:"workspace_id"`
	Prompt         string `json:"prompt" jsonschema:"English only (ASCII). Use printable ASCII plus TAB, LF or CR and include at least one A-Z or a-z letter."`
	IdempotencyKey string `json:"idempotency_key"`
	DeadlineMS     int64  `json:"deadline_ms,omitempty"`
	WaitMS         int64  `json:"wait_ms,omitempty"`
}

type TaskGetArgs struct {
	TaskID string `json:"task_id"`
}

type TaskWaitArgs struct {
	TaskID string `json:"task_id"`
	WaitMS int64  `json:"wait_ms,omitempty"`
}

type TaskCancelArgs struct {
	TaskID string `json:"task_id"`
	Reason string `json:"reason,omitempty"`
}

type TaskListArgs struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	Cursor      int64  `json:"cursor,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type TaskEventsArgs struct {
	TaskID   string `json:"task_id"`
	AfterSeq int64  `json:"after_seq,omitempty"`
	Limit    int    `json:"limit,omitempty"`
}

func (TaskSubmitArgs) clientRequest()        {}
func (TaskRunArgs) clientRequest()           {}
func (TaskGetArgs) clientRequest()           {}
func (TaskWaitArgs) clientRequest()          {}
func (TaskCancelArgs) clientRequest()        {}
func (TaskListArgs) clientRequest()          {}
func (TaskEventsArgs) clientRequest()        {}
func (CommandAuthorizeArgs) clientRequest()  {}
func (ExecutorReconcileArgs) clientRequest() {}

func (TaskSubmitArgs) Method() string        { return "tasks_submit" }
func (TaskRunArgs) Method() string           { return "tasks_run" }
func (TaskGetArgs) Method() string           { return "tasks_get" }
func (TaskWaitArgs) Method() string          { return "tasks_wait" }
func (TaskCancelArgs) Method() string        { return "tasks_cancel" }
func (TaskListArgs) Method() string          { return "tasks_list" }
func (TaskEventsArgs) Method() string        { return "tasks_events" }
func (CommandAuthorizeArgs) Method() string  { return "command_authorize" }
func (ExecutorReconcileArgs) Method() string { return "executor_reconcile" }

func (TaskSubmitArgs) TaskIdentifier() string   { return "" }
func (TaskRunArgs) TaskIdentifier() string      { return "" }
func (a TaskGetArgs) TaskIdentifier() string    { return a.TaskID }
func (a TaskWaitArgs) TaskIdentifier() string   { return a.TaskID }
func (a TaskCancelArgs) TaskIdentifier() string { return a.TaskID }
func (TaskListArgs) TaskIdentifier() string     { return "" }
func (a TaskEventsArgs) TaskIdentifier() string { return a.TaskID }
