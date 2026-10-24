package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/google/uuid"
)

func sessionStoreFixture(t *testing.T) (*sessionStore, model.Task, savedSession) {
	t.Helper()
	store, err := newSessionStore(t.TempDir(), "wss://broker.example/mcp/agent")
	if err != nil {
		t.Fatal(err)
	}
	task := model.Task{ID: uuid.NewString(), WorkspaceID: "workspace", AgentID: "agent"}
	record, err := store.Create(task)
	if err != nil {
		t.Fatal(err)
	}
	return store, task, record
}

func readSessionManifest(t *testing.T, record savedSession) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(record.Directory, sessionManifestName))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestSessionStorePersistsIdentityAndNeverOverwrites(t *testing.T) {
	base := t.TempDir()
	const broker = "wss://broker.example/mcp/agent?private-routing-value=never-persist"
	store, err := newSessionStore(base, broker)
	if err != nil {
		t.Fatal(err)
	}
	task := model.Task{ID: uuid.NewString(), WorkspaceID: "workspace", AgentID: "agent"}
	record, err := store.Create(task)
	if err != nil {
		t.Fatal(err)
	}
	before := readSessionManifest(t, record)
	for _, forbidden := range []string{broker, "never-persist", record.Directory, "mcp.json", "token", "assignment_id"} {
		if bytes.Contains(before, []byte(forbidden)) {
			t.Fatalf("manifest persisted runtime state %q", forbidden)
		}
	}
	if len(record.BrokerKey) != 64 || len(filepath.Base(record.Directory)) != 64 {
		t.Fatalf("broker or task directory key is not a digest: %+v", record)
	}
	if _, err := store.Create(task); !errors.Is(err, ErrSessionExists) {
		t.Fatalf("second Create = %v, want ErrSessionExists", err)
	}
	if after := readSessionManifest(t, record); !bytes.Equal(before, after) {
		t.Fatal("Create replaced the original session manifest")
	}
	restarted, err := newSessionStore(base, broker)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := restarted.Load(task)
	if err != nil {
		t.Fatal(err)
	}
	if loaded != record {
		t.Fatalf("restart changed session identity: got %+v, want %+v", loaded, record)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(filepath.Join(record.Directory, sessionManifestName))
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("manifest permissions: %v, %v", info, err)
		}
	}
}

func TestSessionStoreRejectsMismatchedAndMalformedManifests(t *testing.T) {
	tests := []struct {
		name string
		edit func(map[string]any)
		raw  string
	}{
		{name: "other broker", edit: func(v map[string]any) { v["broker_key"] = strings.Repeat("0", 64) }},
		{name: "other task", edit: func(v map[string]any) { v["task_id"] = uuid.NewString() }},
		{name: "other workspace", edit: func(v map[string]any) { v["workspace_id"] = "other" }},
		{name: "other agent", edit: func(v map[string]any) { v["agent_id"] = "other" }},
		{name: "invalid UUID", edit: func(v map[string]any) { v["session_id"] = "not-a-session" }},
		{name: "zero UUID", edit: func(v map[string]any) { v["session_id"] = uuid.Nil.String() }},
		{name: "unknown version", edit: func(v map[string]any) { v["version"] = 2 }},
		{name: "injected directory", edit: func(v map[string]any) { v["directory"] = "/outside" }},
		{name: "injected MCP config", edit: func(v map[string]any) { v["mcp_config"] = "/outside/mcp.json" }},
		{name: "truncated", raw: `{"version":1,`},
		{name: "two values", raw: `{} {}`},
		{name: "null", raw: `null`},
		{name: "oversized", raw: strings.Repeat(" ", maxSessionManifest+1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, task, record := sessionStoreFixture(t)
			data := []byte(test.raw)
			if test.edit != nil {
				var manifest map[string]any
				if err := json.Unmarshal(readSessionManifest(t, record), &manifest); err != nil {
					t.Fatal(err)
				}
				test.edit(manifest)
				var err error
				data, err = json.Marshal(manifest)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(record.Directory, sessionManifestName), data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Load(task); err == nil {
				t.Fatal("Load accepted a malformed or differently scoped manifest")
			}
		})
	}
}

func TestSessionStoreBindsBrokerAndAuthenticatedTaskIdentity(t *testing.T) {
	store, task, record := sessionStoreFixture(t)
	other, err := newSessionStore(filepath.Dir(store.directory), "wss://other.example/mcp/agent")
	if err != nil {
		t.Fatal(err)
	}
	if other.paths(savedSession{TaskID: task.ID}).Directory == record.Directory {
		t.Fatal("brokers shared a task directory")
	}
	if _, err := other.Load(task); err == nil {
		t.Fatal("another broker loaded the saved session")
	}
	for _, changed := range []model.Task{
		{ID: uuid.NewString(), WorkspaceID: task.WorkspaceID, AgentID: task.AgentID},
		{ID: task.ID, WorkspaceID: "different", AgentID: task.AgentID},
		{ID: task.ID, WorkspaceID: task.WorkspaceID, AgentID: "different"},
		{ID: task.ID, WorkspaceID: task.WorkspaceID},
	} {
		if _, err := store.Load(changed); err == nil {
			t.Fatalf("different task identity loaded the session: %+v", changed)
		}
	}
}

func TestSessionStoreRemovesOnlyItsValidatedTaskDirectory(t *testing.T) {
	store, task, first := sessionStoreFixture(t)
	task.ID = uuid.NewString()
	second, err := store.Create(task)
	if err != nil {
		t.Fatal(err)
	}
	for _, forged := range []savedSession{
		{ID: first.ID, TaskID: first.TaskID, WorkspaceID: first.WorkspaceID, AgentID: first.AgentID, BrokerKey: first.BrokerKey, Directory: second.Directory, MCPConfig: second.MCPConfig},
		{ID: uuid.NewString(), TaskID: first.TaskID, WorkspaceID: first.WorkspaceID, AgentID: first.AgentID, BrokerKey: first.BrokerKey, Directory: first.Directory, MCPConfig: first.MCPConfig},
	} {
		if err := store.Remove(forged); err == nil {
			t.Fatal("Remove accepted a different directory or session UUID")
		}
	}
	if err := store.Remove(first); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(first.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("terminal task directory survived removal: %v", err)
	}
	if _, err := store.Load(task); err != nil {
		t.Fatalf("removing one task damaged its neighbor: %v", err)
	}
	if err := store.Remove(first); err != nil {
		t.Fatalf("repeated terminal cleanup was not idempotent: %v", err)
	}
}

func TestSessionStorePreservesCrashRemnantsAndIgnoresUnpublishedFiles(t *testing.T) {
	store, task, record := sessionStoreFixture(t)
	if err := os.WriteFile(filepath.Join(record.Directory, ".session-abandoned.tmp"), []byte(`{"partial":`), 0600); err != nil {
		t.Fatal(err)
	}
	if loaded, err := store.Load(task); err != nil || loaded.ID != record.ID {
		t.Fatalf("unpublished write damaged the committed manifest: %+v, %v", loaded, err)
	}
	task.ID = uuid.NewString()
	orphan := store.paths(savedSession{TaskID: task.ID})
	if err := os.Mkdir(orphan.Directory, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(task); !errors.Is(err, ErrSessionExists) {
		t.Fatalf("Create overwrote an uncommitted crash remnant: %v", err)
	}
	if _, err := store.Load(task); err == nil {
		t.Fatal("missing committed manifest silently created a replacement session")
	}
}

func TestSessionStoreRejectsSymbolicLinkBoundaries(t *testing.T) {
	for _, target := range []string{"directory", "manifest", "MCP config"} {
		t.Run(target, func(t *testing.T) {
			store, task, record := sessionStoreFixture(t)
			outside := t.TempDir()
			outsideFile := filepath.Join(outside, "preserve.txt")
			data := readSessionManifest(t, record)
			if err := os.WriteFile(outsideFile, data, 0600); err != nil {
				t.Fatal(err)
			}
			var source, destination string
			switch target {
			case "directory":
				if err := os.WriteFile(filepath.Join(outside, sessionManifestName), data, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(record.Directory, record.Directory+".original"); err != nil {
					t.Fatal(err)
				}
				source, destination = outside, record.Directory
			case "manifest":
				destination = filepath.Join(record.Directory, sessionManifestName)
				if err := os.Rename(destination, destination+".original"); err != nil {
					t.Fatal(err)
				}
				source = outsideFile
			case "MCP config":
				source, destination = outsideFile, record.MCPConfig
			}
			if err := os.Symlink(source, destination); err != nil {
				t.Skipf("symbolic links unavailable: %v", err)
			}
			if _, err := store.Load(task); err == nil {
				t.Fatal("Load followed a symbolic link")
			}
			if err := store.Remove(record); err == nil {
				t.Fatal("Remove followed a symbolic link")
			}
			if preserved, err := os.ReadFile(outsideFile); err != nil || !bytes.Equal(data, preserved) {
				t.Fatalf("outside data was modified: %v", err)
			}
		})
	}
}

func TestSessionStoreRejectsSymbolicLinkAtSessionRoot(t *testing.T) {
	base, outside := t.TempDir(), t.TempDir()
	if err := os.Symlink(outside, filepath.Join(base, "sessions")); err != nil {
		t.Skipf("symbolic links unavailable: %v", err)
	}
	if _, err := newSessionStore(base, "wss://broker.example/mcp/agent"); err == nil {
		t.Fatal("store followed a derived session-root symbolic link")
	}
}

func TestSessionStoreCreateClaimsTaskDirectoryOnce(t *testing.T) {
	store, err := newSessionStore(t.TempDir(), "wss://broker.example/mcp/agent")
	if err != nil {
		t.Fatal(err)
	}
	// Raw task identifiers never become path components.
	task := model.Task{ID: `..\..\outside/task`, WorkspaceID: "workspace", AgentID: "agent"}
	type result struct {
		record savedSession
		err    error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			record, err := store.Create(task)
			results <- result{record, err}
		}()
	}
	var succeeded, exists int
	for range 2 {
		result := <-results
		switch {
		case result.err == nil:
			succeeded++
			if filepath.Dir(result.record.Directory) != store.directory {
				t.Fatal("task identifier escaped its derived session directory")
			}
		case errors.Is(result.err, ErrSessionExists):
			exists++
		default:
			t.Fatal(result.err)
		}
	}
	if succeeded != 1 || exists != 1 {
		t.Fatalf("concurrent Create results: succeeded=%d, exists=%d", succeeded, exists)
	}
}
