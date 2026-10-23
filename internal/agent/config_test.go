package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Zxilly/task-bridge/internal/model"
)

func TestAgentConfigResolvesPathsAndRejectsAmbiguousCredentials(t *testing.T) {
	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "agent.json")
	cfg := Config{ServerURL: "ws://127.0.0.1:8080/mcp/agent", TokenFile: "agent.token", ClaudePath: exe, StateDir: "state"}
	if err := os.WriteFile(path, model.JSON(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.TokenFile != filepath.Join(dir, "agent.token") || loaded.StateDir != filepath.Join(dir, "state") {
		t.Fatalf("paths=%+v", loaded)
	}
	cfg.TokenEnv = "BRIDGE_TEST_AGENT_TOKEN"
	if err := cfg.Validate(); err == nil {
		t.Fatal("two credential sources accepted")
	}
	cfg.TokenEnv = ""
	cfg.ServerURL = "ws://example.com/mcp/agent"
	if err := cfg.Validate(); err == nil {
		t.Fatal("remote cleartext accepted")
	}
	if err := os.WriteFile(path, append(model.JSON(loaded), []byte(` {}`)...), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "one JSON") {
		t.Fatalf("extra object accepted: %v", err)
	}
}
