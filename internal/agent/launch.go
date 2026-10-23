package agent

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/processcontrol"
	"github.com/google/uuid"
)

// Session describes one isolated Claude invocation. The worker assigns a new
// UUID and working directory for each task, and owns their cleanup.
type Session struct {
	ID, Directory, MCPConfig, Prompt, Token string
	Task                                    model.ChannelMetadata
}

type Launcher interface {
	Launch(context.Context, Session) error
}

type CommandLauncher struct {
	Path, Model, TokenEnv string
}

// TaskTokenEnv is the only bridge credential inherited by a task subprocess.
const TaskTokenEnv = "BRIDGE_TASK_TOKEN"

const maxLaunchDiagnosticBytes = 16 << 10

func (l CommandLauncher) Launch(ctx context.Context, session Session) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := uuid.Parse(session.ID); err != nil {
		return fmt.Errorf("Claude session ID must be a UUID: %w", err)
	}
	if session.Directory == "" || session.MCPConfig == "" || session.Prompt == "" || session.Token == "" {
		return fmt.Errorf("Claude session directory, MCP config, prompt and task token are required")
	}
	path := l.Path
	if path == "" {
		path = "claude"
	}
	args := []string{
		"--print", "--session-id", session.ID, "--no-session-persistence",
		"--tools", "", "--allowedTools", "mcp__bridge__*",
		"--strict-mcp-config", "--mcp-config", session.MCPConfig,
		"--settings", `{"autoMemoryEnabled":false,"disableAllHooks":true}`,
		"--setting-sources", "user", "--disable-slash-commands",
		"--permission-prompts", "none", "--output-format", "json",
	}
	if l.Model != "" {
		args = append(args, "--model", l.Model)
	}
	cmd := exec.Command(path, args...)
	cmd.Dir = session.Directory
	cmd.Env = l.environment(session.Token)
	cmd.Stdin = strings.NewReader(session.Prompt)
	output := &launchDiagnostic{}
	cmd.Stdout, cmd.Stderr = output, output
	cmd.WaitDelay = 3 * time.Second
	process, err := processcontrol.Prepare(cmd)
	if err != nil {
		return fmt.Errorf("prepare Claude process: %w", err)
	}
	defer process.Close()
	if err = process.Start(cmd); err != nil {
		return fmt.Errorf("start Claude process: %w", err)
	}
	// Reap ordinary descendants even when the CLI exits without cancellation.
	defer process.Terminate(cmd, true)
	if ctx.Err() == nil {
		if err = process.Resume(); err != nil {
			process.Terminate(cmd, true)
			_ = cmd.Wait()
			return fmt.Errorf("resume Claude process: %w", err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err = <-done:
	case <-ctx.Done():
		process.Terminate(cmd, true)
		<-done
		return ctx.Err()
	}
	if err != nil {
		diagnostic := strings.ReplaceAll(output.String(), session.Token, "[redacted]")
		if len(diagnostic) > maxLaunchDiagnosticBytes {
			diagnostic = diagnostic[:maxLaunchDiagnosticBytes] + "\n[output truncated]"
		}
		if diagnostic != "" {
			return fmt.Errorf("Claude process: %w: %s", err, diagnostic)
		}
		return fmt.Errorf("Claude process: %w", err)
	}
	return nil
}

func (l CommandLauncher) environment(token string) []string {
	env := make([]string, 0, len(os.Environ())+1)
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(strings.ToUpper(key), "BRIDGE_") ||
			strings.EqualFold(key, "CLAUDE_CODE_DISABLE_AUTO_MEMORY") ||
			(l.TokenEnv != "" && strings.EqualFold(key, l.TokenEnv)) {
			continue
		}
		env = append(env, entry)
	}
	return append(env, TaskTokenEnv+"="+token, "CLAUDE_CODE_DISABLE_AUTO_MEMORY=1")
}

// launchDiagnostic drains both streams while retaining bounded failure detail.
type launchDiagnostic struct {
	mu        sync.Mutex
	data      []byte
	truncated bool
}

func (d *launchDiagnostic) Write(data []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(data)
	if remaining := maxLaunchDiagnosticBytes - len(d.data); len(data) > remaining {
		data = data[:remaining]
		d.truncated = true
	}
	d.data = append(d.data, data...)
	return n, nil
}

func (d *launchDiagnostic) String() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	text := string(d.data)
	if d.truncated {
		text += "\n[output truncated]"
	}
	return text
}

var _ Launcher = CommandLauncher{}
