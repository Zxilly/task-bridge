package client

import (
	"encoding/json"
	"testing"
)

func TestClientSubmissionToolsAdvertiseModelAndEffort(t *testing.T) {
	session := boundsTestSession(t, (&Client{}).MCPServer())
	list, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range list.Tools {
		if tool.Name != "tasks_submit" && tool.Name != "tasks_run" {
			continue
		}
		data, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatal(err)
		}
		var schema struct {
			Properties map[string]json.RawMessage `json:"properties"`
			Required   []string                   `json:"required"`
		}
		if err := json.Unmarshal(data, &schema); err != nil {
			t.Fatal(err)
		}
		for _, field := range []string{"model", "effort"} {
			if _, ok := schema.Properties[field]; !ok {
				t.Errorf("%s does not expose %s", tool.Name, field)
			}
			for _, required := range schema.Required {
				if required == field {
					t.Errorf("%s must keep %s optional", tool.Name, field)
				}
			}
		}
	}
}
