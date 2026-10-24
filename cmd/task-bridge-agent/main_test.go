package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

func TestOnlyOperationalCommandsAreExposed(t *testing.T) {
	cmd := command()
	commands := make(map[string]bool)
	for _, child := range cmd.Commands() {
		commands[child.Name()] = true
	}
	if commands["resume"] || !commands["run"] || !commands["doctor"] {
		t.Fatalf("unexpected agent operations: %v", commands)
	}
	cmd.SetArgs([]string{"resume"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("CLI still accepted the MCP-only resume operation: %v", err)
	}
}

func TestRunAppendsFileLogAndReportsAuthenticationFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	dir := t.TempDir()
	claudePath := filepath.Join(dir, "claude.exe")
	if err := os.WriteFile(claudePath, []byte("must not be launched"), 0700); err != nil {
		t.Fatal(err)
	}
	const token = "private-agent-file-log-test-token"
	t.Setenv("BRIDGE_TEST_AGENT_TOKEN", token)
	cfg := agent.Config{ServerURL: "ws" + strings.TrimPrefix(server.URL, "http") + "/mcp/agent", ClaudePath: claudePath, StateDir: filepath.Join(dir, "state"), TokenEnv: "BRIDGE_TEST_AGENT_TOKEN"}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	configPath, logPath := filepath.Join(dir, "agent.json"), filepath.Join(dir, "agent.log")
	if err = os.WriteFile(configPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	const previous = "previous run retained\n"
	if err = os.WriteFile(logPath, []byte(previous), 0600); err != nil {
		t.Fatal(err)
	}
	var diagnostic bytes.Buffer
	cmd := command()
	cmd.SetErr(&diagnostic)
	cmd.SetArgs([]string{"run", "--config", configPath, "--log-file", logPath})
	if err = cmd.Execute(); err == nil || !strings.Contains(err.Error(), "credential rejected") {
		t.Fatalf("expected authentication failure, got %v", err)
	}
	logged, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(logged), previous) {
		t.Fatal("starting the controller discarded the previous log")
	}
	var entry struct{ Level, Msg, Error string }
	if err = json.Unmarshal(bytes.TrimPrefix(logged, []byte(previous)), &entry); err != nil {
		t.Fatalf("controller diagnostic is not one JSON log entry: %v", err)
	}
	if entry.Level != "ERROR" || entry.Msg != "agent stopped" || !strings.Contains(entry.Error, "credential rejected") {
		t.Fatalf("unexpected controller log: %+v", entry)
	}
	if !strings.Contains(diagnostic.String(), "agent stopped") || strings.Contains(string(logged)+diagnostic.String(), token) {
		t.Fatal("controller failed to retain stderr diagnostics or exposed its credential")
	}
	// Windows GUI-subsystem processes started by Task Scheduler may have no
	// usable stderr handle. That must not prevent the file from receiving logs.
	cmd = command()
	cmd.SetErr(unavailableDiagnosticWriter{})
	cmd.SetArgs([]string{"run", "--config", configPath, "--log-file", logPath})
	if err = cmd.Execute(); err == nil || !strings.Contains(err.Error(), "credential rejected") {
		t.Fatalf("expected authentication failure without a console, got %v", err)
	}
	extended, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(bytes.TrimPrefix(extended, logged), &entry); err != nil {
		t.Fatalf("file diagnostic was lost when stderr was unavailable: %v", err)
	}
}

type unavailableDiagnosticWriter struct{}

func (unavailableDiagnosticWriter) Write([]byte) (int, error) { return 0, os.ErrInvalid }
