package broker

import (
	"encoding/json"

	"github.com/Zxilly/task-bridge/internal/model"
)

// Request keys retain the original positional JSON arrays because their hashes
// are persisted for idempotency checks across broker upgrades.
type commandRequestKey struct {
	Command    string
	Cwd        string
	TimeoutMS  int64
	Background bool
}

func (k commandRequestKey) MarshalJSON() ([]byte, error) {
	parts := []json.RawMessage{model.JSON(k.Command), model.JSON(k.Cwd), model.JSON(k.TimeoutMS)}
	if k.Background {
		parts = append(parts, model.JSON(true))
	}
	return json.Marshal(parts)
}

type taskRequestKey struct {
	WorkspaceID string
	Prompt      string
	DeadlineMS  int64
	Model       string
	Effort      string
}

func (k taskRequestKey) MarshalJSON() ([]byte, error) {
	parts := []json.RawMessage{model.JSON(k.WorkspaceID), model.JSON(k.Prompt), model.JSON(k.DeadlineMS)}
	// Retain the persisted three-element key when no overrides were supplied.
	if k.Model != "" || k.Effort != "" {
		parts = append(parts, model.JSON(k.Model), model.JSON(k.Effort))
	}
	return json.Marshal(parts)
}
