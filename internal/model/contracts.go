package model

// AgentResult is the closed set of values returned by agent tool operations.
// Concrete values retain their existing JSON object shape at the transport.
type AgentResult interface{ agentResult() }

func (Task) agentResult()          {}
func (TaskSnapshot) agentResult()  {}
func (Command) agentResult()       {}
func (CommandResult) agentResult() {}
func (OutputResult) agentResult()  {}

// ClientResult is the closed set of values returned to an executor connection.
type ClientResult interface{ clientResult() }

func (Task) clientResult()                    {}
func (TaskSnapshot) clientResult()            {}
func (TaskWaitResult) clientResult()          {}
func (TaskListResult) clientResult()          {}
func (CommandAuthorization) clientResult()    {}
func (ExecutorReconcileResult) clientResult() {}

// EventList remains a JSON array, including the distinction between nil and [].
type EventList []Event

func (EventList) clientResult() {}

// AdminResult is the closed set of administrative operation results.
type AdminResult interface{ adminResult() }

func (DeviceCreateResult) adminResult() {}
func (Workspace) adminResult()          {}
func (RevocationResult) adminResult()   {}
func (BackupResult) adminResult()       {}
func (JournalResetResult) adminResult() {}
func (TaskSnapshot) adminResult()       {}
func (Task) adminResult()               {}
func (Command) adminResult()            {}

type PrincipalList []Principal

func (PrincipalList) adminResult() {}

// AdminRequestArgs limits CLI requests to the supported argument contracts.
type AdminRequestArgs interface{ adminRequestArgs() }

func (AdminArgs) adminRequestArgs() {}
func (Principal) adminRequestArgs() {}
func (Workspace) adminRequestArgs() {}

// EventPayload restricts durable task events to their versioned payloads.
type EventPayload interface{ eventPayload() }

func (TaskStatePayload) eventPayload()           {}
func (AssignmentPayload) eventPayload()          {}
func (CommandIDPayload) eventPayload()           {}
func (TaskProgressPayload) eventPayload()        {}
func (TaskCompletedPayload) eventPayload()       {}
func (TaskCancelRequestedPayload) eventPayload() {}
func (CommandStatePayload) eventPayload()        {}

// OutboxPayload restricts durable deliveries to executable protocol messages.
type OutboxPayload interface{ outboxPayload() }

func (Dispatch) outboxPayload()     {}
func (CommandEvent) outboxPayload() {}
