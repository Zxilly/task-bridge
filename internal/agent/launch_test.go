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
	"slices"
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
	MCPConfig   json.RawMessage
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
		for _, key := range []string{"BRIDGE_OLD_TOKEN", "bridge_old_lower", "CUSTOM_AGENT_CREDENTIAL", TaskTokenEnv, "CLAUDE_CODE_OAUTH_TOKEN", "CLAUDE_CONFIG_DIR", "CLAUDE_CODE_DISABLE_AUTO_MEMORY", "CLAUDE_CODE_SKIP_PROMPT_HISTORY", "CLAUDE_CODE_RESUME_INTERRUPTED_TURN", "CLAUDE_CODE_EFFORT_LEVEL"} {
			if value, ok := os.LookupEnv(key); ok {
				report.Environment[key] = value
			}
		}
		if index := slices.Index(report.Args, "--mcp-config"); index >= 0 && index+1 < len(report.Args) {
			report.MCPConfig, err = os.ReadFile(report.Args[index+1])
			if err != nil {
				return err
			}
		}
		data, err := json.Marshal(report)
		if err != nil {
			return err
		}
		return os.WriteFile(os.Getenv("TASK_BRIDGE_LAUNCH_TEST_REPORT"), data, 0600)
	case "resume-missing":
		record, err := os.OpenFile(os.Getenv("TASK_BRIDGE_LAUNCH_TEST_REPORT"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		err = json.NewEncoder(record).Encode(os.Args[1:])
		closeErr := record.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		fmt.Fprintln(os.Stderr, "No conversation found with session ID:", os.Args[3])
		os.Exit(18)
	case "fail":
		fmt.Fprint(os.Stdout, strings.Repeat("verbose output ", 4096))
		fmt.Fprintln(os.Stderr, "controlled CLI failure", os.Getenv(TaskTokenEnv))
		fmt.Fprint(os.Stderr, strings.Repeat("x", 64<<10))
		os.Exit(17)
	case "auth-error", "rate-error":
		// Reproduce a CLI failure after verbose stdout has filled the ordinary
		// diagnostic buffer. The structured error must still reach B safely.
		fmt.Fprintln(os.Stdout, strings.Repeat("x", 32<<10))
		kind, status := "authentication_failed", 403
		if os.Getenv("TASK_BRIDGE_LAUNCH_TEST_MODE") == "rate-error" {
			kind, status = "rate_limit", 429
		}
		fmt.Fprintf(os.Stdout, `{"type":"assistant","session_id":%q,"is_api_error_message":true,"error":%q,"api_error_status":%d,"message":{"content":[{"type":"text","text":"private diagnostic %s"}]}}`+"\n", os.Args[3], kind, status, os.Getenv(TaskTokenEnv))
		fmt.Fprintf(os.Stdout, `{"type":"result","session_id":%q,"is_error":true,"api_error_status":%d,"result":"private result %s"}`, os.Args[3], status, os.Getenv(TaskTokenEnv))
		os.Exit(1)
	case "usage":
		// Emit enough ordinary output to fill the failure diagnostic buffer.
		fmt.Fprintln(os.Stdout, strings.Repeat("x", 32<<10))
		fmt.Fprintf(os.Stdout, `{"type":"rate_limit_event","session_id":%q,"rate_limit_info":{"status":"allowed","unifiedWindows":{"five_hour":{"utilization":0.24,"resetsAt":4102444800},"seven_day":{"utilization":0.13,"resetsAt":4102444800}}}}`+"\n", os.Args[3])
		fmt.Fprintf(os.Stderr, `{"type":"rate_limit_event","session_id":%q,"rate_limit_info":{"status":"rejected","rateLimitType":"five_hour","utilization":1}}`+"\n", os.Args[3])
		time.Sleep(30 * time.Second)
	case "usage-query":
		if err := os.WriteFile(os.Getenv("TASK_BRIDGE_LAUNCH_TEST_REPORT"), []byte(strings.Join(os.Args[1:], "\n")), 0600); err != nil {
			return err
		}
		reset := time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
		fmt.Fprintf(os.Stdout, `{"type":"assistant","usage_report":{"session":{"total_cost_usd":0,"total_api_duration_ms":0,"model_usage":{}},"rate_limits":{"limits":[{"kind":"session","group":"session","percent":24,"resets_at":%q,"is_active":true},{"kind":"weekly_all","group":"weekly","percent":38,"resets_at":%q,"is_active":false}]}}}`+"\n", reset, reset)
		fmt.Fprintln(os.Stdout, `{"type":"result","is_error":false,"num_turns":0,"total_cost_usd":0}`)
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
	t.Setenv("CLAUDE_CODE_SKIP_PROMPT_HISTORY", "1")
	t.Setenv("CLAUDE_CODE_RESUME_INTERRUPTED_TURN", "1")
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(t.TempDir(), "existing-claude-config"))
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
			"--print", "--session-id", session.ID,
			"--tools", "", "--allowedTools", "mcp__bridge__*",
			"--strict-mcp-config", "--mcp-config", session.MCPConfig,
			"--settings", `{"autoMemoryEnabled":false,"disableAllHooks":true,"env":{"CLAUDE_CODE_SKIP_PROMPT_HISTORY":"0","CLAUDE_CODE_RESUME_INTERRUPTED_TURN":"0"}}`,
			"--setting-sources", "user", "--disable-slash-commands",
			"--permission-prompts", "none", "--output-format", "stream-json", "--verbose", "--model", "test-model",
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
		if _, ok := report.Environment["CLAUDE_CODE_SKIP_PROMPT_HISTORY"]; ok {
			t.Fatal("inherited environment disabled transcript persistence")
		}
		if report.Environment["CLAUDE_CODE_RESUME_INTERRUPTED_TURN"] != "0" {
			t.Fatal("inherited environment enabled automatic interrupted-turn continuation")
		}
		if report.Environment["CLAUDE_CONFIG_DIR"] != os.Getenv("CLAUDE_CONFIG_DIR") {
			t.Fatal("existing Claude configuration and login location changed")
		}
		if previous.ID == session.ID || previous.Directory == session.Directory {
			t.Fatal("two tasks shared a session ID or working directory")
		}
		previous = session
	}
}

func TestCommandLauncherResumesSameSessionWithCurrentRelay(t *testing.T) {
	launcher, reportPath := launcherHelper(t, "record")
	session := launcherSession(t)
	if err := launcher.Launch(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	var first launchReport
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &first); err != nil {
		t.Fatal(err)
	}

	session.Resume = true
	session.MCPConfig = filepath.Join(session.Directory, "resumed relay.json")
	session.Prompt = "Resume this task using the current assignment; inspect known command results before executing."
	session.Token = "resumed-task-token-" + uuid.NewString()
	config := `{"mcpServers":{"bridge":{"type":"http","url":"http://127.0.0.1:43210/current-run","headers":{"Authorization":"Bearer ${BRIDGE_TASK_TOKEN}"}}}}`
	if err := os.WriteFile(session.MCPConfig, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	if err := launcher.Launch(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	var resumed launchReport
	data, err = os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &resumed); err != nil {
		t.Fatal(err)
	}
	if len(resumed.Args) < 3 || !reflect.DeepEqual(resumed.Args[:3], []string{"--print", "--resume", session.ID}) || first.Args[2] != session.ID {
		t.Fatalf("resume did not select the original session: first=%q resumed=%q", first.Args, resumed.Args)
	}
	for _, forbidden := range []string{"--session-id", "--continue", "--fork-session", "--no-session-persistence"} {
		if slices.Contains(resumed.Args, forbidden) {
			t.Fatalf("resume included conflicting or nonpersistent flag %s", forbidden)
		}
	}
	if resumed.Directory != first.Directory || resumed.Directory != session.Directory {
		t.Fatalf("resume changed task cwd: first=%q resumed=%q", first.Directory, resumed.Directory)
	}
	if resumed.Prompt != session.Prompt || resumed.Environment[TaskTokenEnv] != session.Token || resumed.Environment[TaskTokenEnv] == first.Environment[TaskTokenEnv] {
		t.Fatal("resume reused an old task prompt or relay credential")
	}
	if string(resumed.MCPConfig) != config || reflect.DeepEqual(resumed.MCPConfig, first.MCPConfig) {
		t.Fatal("resume did not read the current invocation's MCP configuration")
	}
}

func TestCommandLauncherMissingResumeDoesNotStartFreshSession(t *testing.T) {
	launcher, reportPath := launcherHelper(t, "resume-missing")
	session := launcherSession(t)
	session.Resume = true
	err := launcher.Launch(t.Context(), session)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 18 || !strings.Contains(err.Error(), "No conversation found with session ID: "+session.ID) {
		t.Fatalf("missing session error was not preserved: %v", err)
	}
	record, err := os.Open(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	defer record.Close()
	decoder := json.NewDecoder(record)
	var args []string
	if err := decoder.Decode(&args); err != nil {
		t.Fatal(err)
	}
	if len(args) < 3 || !reflect.DeepEqual(args[:3], []string{"--print", "--resume", session.ID}) {
		t.Fatalf("failed invocation did not select original session: %q", args)
	}
	if err := decoder.Decode(new([]string)); !errors.Is(err, io.EOF) {
		t.Fatalf("launcher retried after missing resume: %v", err)
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
