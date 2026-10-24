package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Zxilly/task-bridge/internal/agent"
)

func TestDoctorDoesNotLaunchClaudeOrCreateState(t *testing.T) {
	dir := t.TempDir()
	// LookPath accepts this file, but attempting to launch it would fail on
	// every platform. Doctor must only validate the configured path.
	claudePath := filepath.Join(dir, "claude.exe")
	if err := os.WriteFile(claudePath, []byte("not an executable program"), 0700); err != nil {
		t.Fatal(err)
	}
	const token = "private-agent-test-token"
	t.Setenv("BRIDGE_TEST_AGENT_TOKEN", token)
	cfg := agent.Config{ServerURL: "ws://127.0.0.1:1/mcp/agent", ClaudePath: claudePath, StateDir: filepath.Join(dir, "state"), TokenEnv: "BRIDGE_TEST_AGENT_TOKEN"}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "agent.json")
	if err = os.WriteFile(configPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	var output, diagnostic bytes.Buffer
	cmd := command()
	cmd.SetOut(&output)
	cmd.SetErr(&diagnostic)
	cmd.SetArgs([]string{"doctor", "--config", configPath})
	if err = cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	var status doctorStatus
	if err = json.Unmarshal(output.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if !status.OK || !status.CredentialPresent || status.ClaudeStarted || status.NetworkChecked {
		t.Fatalf("unexpected doctor status: %+v", status)
	}
	if strings.Contains(output.String()+diagnostic.String(), token) {
		t.Fatal("doctor exposed the credential")
	}
	if _, err = os.Stat(cfg.StateDir); !os.IsNotExist(err) {
		t.Fatalf("doctor created state directory: %v", err)
	}
}

func TestResumeRequiresAnExplicitTaskID(t *testing.T) {
	cmd := command()
	cmd.SetArgs([]string{"resume"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "task-id") {
		t.Fatalf("resume without a task ID was not rejected: %v", err)
	}
}
