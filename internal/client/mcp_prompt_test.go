package client

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Zxilly/task-bridge/internal/broker"
	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestClientSubmissionToolsAdvertiseEnglishPromptContract(t *testing.T) {
	session := boundsTestSession(t, (&Client{}).MCPServer())
	list, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	found := make(map[string]bool)
	for _, tool := range list.Tools {
		if tool.Name != "tasks_submit" && tool.Name != "tasks_run" {
			continue
		}
		found[tool.Name] = true
		if !strings.Contains(tool.Description, "English only (ASCII)") {
			t.Errorf("%s description omits English requirement: %s", tool.Name, tool.Description)
		}
		data, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatal(err)
		}
		var schema struct {
			Properties map[string]struct {
				Description string `json:"description"`
			} `json:"properties"`
		}
		if err := json.Unmarshal(data, &schema); err != nil {
			t.Fatal(err)
		}
		description := schema.Properties["prompt"].Description
		if !strings.Contains(description, "English only (ASCII)") || !strings.Contains(description, "TAB") || !strings.Contains(description, "A-Z") {
			t.Errorf("%s prompt schema omits deterministic requirement: %s", tool.Name, description)
		}
	}
	if len(found) != 2 {
		t.Fatalf("missing submission tools: %+v", found)
	}
}

func TestClientSubmissionToolsUseBrokerEnglishValidation(t *testing.T) {
	for _, method := range []string{"tasks_submit", "tasks_run"} {
		t.Run(method, func(t *testing.T) {
			store, err := storage.Open(t.Context(), filepath.Join(t.TempDir(), "broker.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			b := broker.New(store, 10)
			if err := b.CreateWorkspace(t.Context(), model.Workspace{ID: "local", ExecutorID: "executor"}); err != nil {
				t.Fatal(err)
			}
			principal, _, err := b.CreateDevice(t.Context(), model.Principal{Kind: "executor", ExecutorID: "executor"})
			if err != nil {
				t.Fatal(err)
			}
			peer, err := b.ConnectClient(t.Context(), principal, "test-journal", func(model.Envelope) error { return nil }, func() {})
			if err != nil {
				t.Fatal(err)
			}
			c, ctx := clientMCPTestPeer(t, func(rpc model.RPC) model.Envelope {
				result, err := b.CallClient(t.Context(), peer, rpc.Method, rpc.Args)
				if err != nil {
					return model.Envelope{Error: model.AsError(err)}
				}
				return model.Envelope{Payload: model.JSON(result)}
			})
			session := clientMCPTestSession(t, ctx, c, nil)
			arguments := func(prompt, key string) model.ClientRequest {
				if method == "tasks_run" {
					return RunArgs{WorkspaceID: "local", Prompt: prompt, IdempotencyKey: key, WaitMS: 1}
				}
				return SubmitArgs{WorkspaceID: "local", Prompt: prompt, IdempotencyKey: key}
			}
			for i, prompt := range []string{"检查工作区", "Run 中文 tests", "Run\x00tests", "1234!?"} {
				result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: method, Arguments: arguments(prompt, fmt.Sprintf("reject-%d", i))})
				if err != nil {
					t.Fatal(err)
				}
				body := clientMCPResultValue[model.ErrorResult](t, result)
				if !result.IsError || body.Error == nil || body.Error.Code != "invalid_arguments" || !strings.Contains(body.Error.Message, "English only (ASCII)") {
					t.Fatalf("broker validation was lost: %+v", body)
				}
			}
			if err := store.View(ctx, func(tx *storage.Tx) error {
				var count int
				if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM tasks").Scan(&count); err != nil {
					return err
				}
				if count != 0 {
					t.Fatalf("rejected MCP prompt created tasks: %d", count)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			const english = "Inspect the workspace.\r\n\tRun tests and report the result."
			result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: method, Arguments: arguments(english, "accepted")})
			if err != nil || result.IsError {
				t.Fatalf("English MCP submission failed: result=%+v error=%v", result, err)
			}
			// Simulate a task stored before the new prompt policy. The durable
			// request key has always used [workspace, prompt, normalized deadline].
			const legacyPrompt = "检查旧任务"
			var legacyID string
			if err := store.Update(ctx, func(tx *storage.Tx) error {
				task, err := tx.TaskByKey(principal.ID, "accepted")
				if err != nil {
					return err
				}
				if task.Prompt != english {
					t.Fatalf("English prompt changed: %q", task.Prompt)
				}
				task.ID = uuid.NewString()
				task.IdempotencyKey = "legacy"
				legacyID = task.ID
				task.Prompt = legacyPrompt
				task.RequestHash = storage.Hash(string(model.JSON([3]json.RawMessage{model.JSON("local"), model.JSON(legacyPrompt), model.JSON(1800000)})))
				return tx.SaveTask(task)
			}); err != nil {
				t.Fatal(err)
			}
			result, err = session.CallTool(ctx, &mcp.CallToolParams{Name: method, Arguments: arguments(legacyPrompt, "legacy")})
			if err != nil || result.IsError {
				t.Fatalf("MCP blocked historical retry: result=%+v error=%v", result, err)
			}
			data := clientMCPResultJSON(t, result)
			if !strings.Contains(string(data), legacyID) {
				t.Fatalf("historical retry lost original ID: %s", data)
			}
		})
	}
}
