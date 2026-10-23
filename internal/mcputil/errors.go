package mcputil

import (
	"encoding/json"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// JSONTaskID inspects only routing fields at an open JSON transport boundary.
func JSONTaskID(data json.RawMessage) string {
	var identity struct {
		TaskID string `json:"task_id"`
		Task   struct {
			ID string `json:"task_id"`
		} `json:"task"`
	}
	if json.Unmarshal(data, &identity) == nil {
		if identity.TaskID != "" {
			return identity.TaskID
		}
		return identity.Task.ID
	}
	return ""
}

// SmallError keeps a corrupt routing identifier from defeating the size cap.
func SmallError(code, message, taskID string) *mcp.CallToolResult {
	value := model.ErrorResult{Error: &model.Error{Code: code, Message: message}, TaskID: taskID}
	result := &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(model.JSON(value))}}, IsError: true}
	if wire, err := json.Marshal(result); err != nil || len(wire) > model.MaxToolBytes {
		value.TaskID = ""
		result.Content = []mcp.Content{&mcp.TextContent{Text: string(model.JSON(value))}}
	}
	return result
}
