package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

const launchTestHelperEnv = "TASK_BRIDGE_LAUNCH_TEST_HELPER"

type launchReport struct {
	Args        []string
	Directory   string
	Prompt      string
	Environment map[string]string
}

// The helper handles Claude-shaped arguments before testing's flag parser.
// It exercises the actual executable boundary without starting a model.
func init() {
	if os.Getenv(launchTestHelperEnv) != "1" {
		return
	}
	if err := runLaunchTestHelper(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(99)
	}
	os.Exit(0)
}

func runLaunchTestHelper() error {
	switch os.Getenv("TASK_BRIDGE_LAUNCH_TEST_MODE") {
	case "record":
		prompt, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		directory, err := os.Getwd()
		if err != nil {
			return err
		}
		report := launchReport{Args: os.Args[1:], Directory: directory, Prompt: string(prompt), Environment: make(map[string]string)}
		for _, key := range []string{"BRIDGE_OLD_TOKEN", "bridge_old_lower", "CUSTOM_AGENT_CREDENTIAL", TaskTokenEnv, "CLAUDE_CODE_OAUTH_TOKEN", "CLAUDE_CODE_DISABLE_AUTO_MEMORY"} {
			if value, ok := os.LookupEnv(key); ok {
				report.Environment[key] = value
			}
		}
		data, err := json.Marshal(report)
		if err != nil {
			return err
		}
		return os.WriteFile(os.Getenv("TASK_BRIDGE_LAUNCH_TEST_REPORT"), data, 0600)
	case "fail":
		fmt.Fprintln(os.Stderr, "controlled CLI failure", os.Getenv(TaskTokenEnv))
		fmt.Fprint(os.Stderr, strings.Repeat("x", 64<<10))
		os.Exit(17)
	case "tree":
		child := exec.Command(os.Args[0])
		child.Env = append(os.Environ(), "TASK_BRIDGE_LAUNCH_TEST_MODE=descendant")
		child.Stdout, child.Stderr = os.Stdout, os.Stderr
		if err := child.Start(); err != nil {
			return err
		}
		return child.Wait()
	case "descendant":
		if err := os.WriteFile(os.Getenv("TASK_BRIDGE_LAUNCH_TEST_REPORT"), []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
			return err
		}
		time.Sleep(2 * time.Second)
		return os.WriteFile(os.Getenv("TASK_BRIDGE_LAUNCH_TEST_MARKER"), []byte("descendant survived"), 0600)
	default:
		return errors.New("unknown launcher helper mode")
	}
	return nil
}

func launcherHelper(t *testing.T, mode string) (CommandLauncher, string) {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(launchTestHelperEnv, "1")
	t.Setenv("TASK_BRIDGE_LAUNCH_TEST_MODE", mode)
	report := filepath.Join(t.TempDir(), "launch report.json")
	t.Setenv("TASK_BRIDGE_LAUNCH_TEST_REPORT", report)
	return CommandLauncher{Path: path, Model: "test-model", TokenEnv: "CUSTOM_AGENT_CREDENTIAL"}, report
}

func launcherSession(t *testing.T) Session {
	t.Helper()
	directory := t.TempDir()
	config := filepath.Join(directory, "task mcp.json")
	if err := os.WriteFile(config, []byte(`{"mcpServers":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	return Session{
		ID: uuid.NewString(), Directory: directory, MCPConfig: config,
		Prompt: "A fresh task\n你好 --not-a-flag", Token: "task-token-" + uuid.NewString(),
	}
}

func TestCommandLauncherIsolatesArgumentsEnvironmentAndDirectory(t *testing.T) {
	launcher, reportPath := launcherHelper(t, "record")
	t.Setenv("BRIDGE_OLD_TOKEN", "old-broker-token")
	t.Setenv("bridge_old_lower", "old-lowercase-token")
	t.Setenv(TaskTokenEnv, "old-task-token")
	t.Setenv("CUSTOM_AGENT_CREDENTIAL", "old-custom-broker-token")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "existing-claude-login")
	t.Setenv("CLAUDE_CODE_DISABLE_AUTO_MEMORY", "0")
	var previous Session
	for range 2 {
		session := launcherSession(t)
		if err := launcher.Launch(t.Context(), session); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(reportPath)
		if err != nil {
			t.Fatal(err)
		}
		var report launchReport
		if err := json.Unmarshal(data, &report); err != nil {
			t.Fatal(err)
		}
		wantArgs := []string{
			"--print", "--session-id", session.ID, "--no-session-persistence",
			"--tools", "", "--allowedTools", "mcp__bridge__*",
			"--strict-mcp-config", "--mcp-config", session.MCPConfig,
			"--settings", `{"autoMemoryEnabled":false,"disableAllHooks":true}`,
			"--setting-sources", "user", "--disable-slash-commands",
			"--permission-prompts", "none", "--output-format", "json", "--model", "test-model",
		}
		if !reflect.DeepEqual(report.Args, wantArgs) {
			t.Fatalf("arguments = %#v, want %#v", report.Args, wantArgs)
		}
		if report.Directory != session.Directory || report.Prompt != session.Prompt {
			t.Fatalf("directory or stdin changed: %+v", report)
		}
		for _, key := range []string{"BRIDGE_OLD_TOKEN", "bridge_old_lower", "CUSTOM_AGENT_CREDENTIAL"} {
			if _, ok := report.Environment[key]; ok {
				t.Fatalf("broker credential %q was inherited", key)
			}
		}
		if report.Environment[TaskTokenEnv] != session.Token || report.Environment["CLAUDE_CODE_OAUTH_TOKEN"] != "existing-claude-login" {
			t.Fatalf("task credential or existing Claude login was lost: %+v", report.Environment)
		}
		if report.Environment["CLAUDE_CODE_DISABLE_AUTO_MEMORY"] != "1" {
			t.Fatal("inherited environment enabled shared auto memory")
		}
		if previous.ID == session.ID || previous.Directory == session.Directory {
			t.Fatal("two tasks shared a session ID or working directory")
		}
		previous = session
	}
}

func TestCommandLauncherReportsBoundedNonzeroExit(t *testing.T) {
	launcher, _ := launcherHelper(t, "fail")
	session := launcherSession(t)
	err := launcher.Launch(t.Context(), session)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 17 {
		t.Fatalf("Launch error = %v, want exit 17", err)
	}
	diagnostic := err.Error()
	if !strings.Contains(diagnostic, "controlled CLI failure") || !strings.Contains(diagnostic, "[output truncated]") {
		t.Fatalf("failure detail missing: %v", err)
	}
	if len(diagnostic) > maxLaunchDiagnosticBytes+200 {
		t.Fatalf("failure diagnostic is unbounded: %d bytes", len(diagnostic))
	}
	if strings.Contains(diagnostic, session.Token) {
		t.Fatal("failure diagnostic leaked the task credential")
	}
}

func TestCommandLauncherCancellationStopsProcessTree(t *testing.T) {
	launcher, ready := launcherHelper(t, "tree")
	marker := filepath.Join(t.TempDir(), "orphan marker.txt")
	t.Setenv("TASK_BRIDGE_LAUNCH_TEST_MARKER", marker)
	ctx, cancel := context.WithCancel(t.Context())
	session := launcherSession(t)
	finished := make(chan struct{})
	var launchErr error
	go func() {
		launchErr = launcher.Launch(ctx, session)
		close(finished)
	}()
	defer func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("launcher cleanup did not finish")
		}
	}()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		select {
		case <-finished:
			t.Fatalf("launcher exited before its descendant started: %v", launchErr)
		case <-deadline.C:
			t.Fatal("descendant did not start")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-finished:
		if !errors.Is(launchErr, context.Canceled) {
			t.Fatalf("Launch error = %v, want context.Canceled", launchErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not promptly stop the CLI")
	}
	time.Sleep(2200 * time.Millisecond)
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("descendant survived cancellation: %v", err)
	}
}

func TestCommandLauncherAlreadyCanceledDoesNotStart(t *testing.T) {
	launcher, report := launcherHelper(t, "record")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := launcher.Launch(ctx, launcherSession(t)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Launch error = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(report); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pre-canceled invocation started a process: %v", err)
	}
}
