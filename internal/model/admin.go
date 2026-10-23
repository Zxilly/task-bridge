package model

// AdminArgs contains the administrative parameters shared by the CLI and broker.
// Device and workspace creation use their own domain contracts.
type AdminArgs struct {
	ID      string `json:"id,omitempty"`
	Outcome string `json:"outcome,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Output  string `json:"output,omitempty"`
}

// AdminResponse wraps the result of an administrative operation. The operation
// selects the result type; the CLI retains its JSON for display.
type AdminResponse[T any] struct {
	Result T `json:"result"`
}
