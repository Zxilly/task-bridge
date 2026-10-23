package broker

import (
	"encoding/json"

	"github.com/Zxilly/task-bridge/internal/model"
)

// Request keys retain the original positional JSON arrays because their hashes
// are persisted for idempotency checks across broker upgrades.
type commandRequestKey struct {
	Command   string
	Cwd       string
	TimeoutMS int64
}

func (k commandRequestKey) MarshalJSON() ([]byte, error) {
	return json.Marshal([3]json.RawMessage{model.JSON(k.Command), model.JSON(k.Cwd), model.JSON(k.TimeoutMS)})
}

type taskRequestKey struct {
	WorkspaceID string
	Prompt      string
	DeadlineMS  int64
}

func (k taskRequestKey) MarshalJSON() ([]byte, error) {
	return json.Marshal([3]json.RawMessage{model.JSON(k.WorkspaceID), model.JSON(k.Prompt), model.JSON(k.DeadlineMS)})
}
