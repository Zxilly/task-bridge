package agent

import (
	"context"
	"errors"
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

// Session describes one Claude invocation. A task keeps its UUID and working
// directory when resumed, while each invocation receives a new relay credential.
type Session struct {
	ID, Directory, MCPConfig, Prompt, Token string
	Task                                    model.ChannelMetadata
	Resume                                  bool
	Model, Effort                           string
	Usage                                   *accountUsage
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
	if err := model.ValidateTaskOptions(session.Model, session.Effort); err != nil {
		return err
	}
	path := l.Path
	if path == "" {
		path = "claude"
	}
	sessionFlag := "--session-id"
	if session.Resume {
		sessionFlag = "--resume"
	}
	args := []string{
		"--print", sessionFlag, session.ID,
		"--tools", "", "--allowedTools", "mcp__bridge__*",
		"--strict-mcp-config", "--mcp-config", session.MCPConfig,
		"--settings", `{"autoMemoryEnabled":false,"disableAllHooks":true,"env":{"CLAUDE_CODE_SKIP_PROMPT_HISTORY":"0","CLAUDE_CODE_RESUME_INTERRUPTED_TURN":"0"}}`,
		"--setting-sources", "user", "--disable-slash-commands",
		"--permission-prompts", "none", "--output-format", "stream-json", "--verbose",
	}
	modelName := session.Model
	if modelName == "" {
		modelName = l.Model
	}
	if modelName != "" {
		args = append(args, "--model", modelName)
	}
	if session.Effort != "" {
		args = append(args, "--effort", session.Effort)
	}
	cmd := exec.Command(path, args...)
	cmd.Dir = session.Directory
	cmd.Env = l.environment(session.Token, session.Effort)
	cmd.Stdin = strings.NewReader(session.Prompt)
	output := &launchDiagnostic{}
	outcome := &launchOutcome{}
	stream := &usageStream{diagnostic: &launchDiagnostic{}, usage: session.Usage, sessionID: session.ID, outcome: outcome}
	defer stream.finish()
	cmd.Stdout, cmd.Stderr = stream, output
	cmd.WaitDelay = 3 * time.Second
	process, err := processcontrol.PrepareForParentExit(cmd)
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
	stream.finish()
	if err == nil && outcome.failure != nil {
		err = errors.New("Claude reported an unsuccessful result")
	}
	if err != nil {
		// Verbose stdout can fill its buffer before a later stderr failure.
		// Preserve stderr first, while still including stdout-only CLI errors.
		diagnostic := output.String()
		if stdout := stream.diagnostic.String(); stdout != "" {
			if diagnostic != "" {
				diagnostic += "\n"
			}
			diagnostic += stdout
		}
		diagnostic = strings.ReplaceAll(diagnostic, session.Token, "[redacted]")
		if len(diagnostic) > maxLaunchDiagnosticBytes {
			diagnostic = diagnostic[:maxLaunchDiagnosticBytes] + "\n[output truncated]"
		}
		if diagnostic != "" {
			err = fmt.Errorf("Claude process: %w: %s", err, diagnostic)
		} else {
			err = fmt.Errorf("Claude process: %w", err)
		}
		if outcome.failure != nil {
			return &launchFailure{public: *outcome.failure, cause: err}
		}
		return err
	}
	return nil
}

func (l CommandLauncher) environment(token, effort string) []string {
	env := make([]string, 0, len(os.Environ())+1)
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(strings.ToUpper(key), "BRIDGE_") ||
			strings.EqualFold(key, "CLAUDE_CODE_DISABLE_AUTO_MEMORY") ||
			strings.EqualFold(key, "CLAUDE_CODE_SKIP_PROMPT_HISTORY") ||
			strings.EqualFold(key, "CLAUDE_CODE_RESUME_INTERRUPTED_TURN") ||
			(effort != "" && strings.EqualFold(key, "CLAUDE_CODE_EFFORT_LEVEL")) ||
			(l.TokenEnv != "" && strings.EqualFold(key, l.TokenEnv)) {
			continue
		}
		env = append(env, entry)
	}
	if effort != "" {
		// The environment override can take precedence over --effort. Replace
		// an inherited value so a task's explicit choice always reaches Claude.
		env = append(env, "CLAUDE_CODE_EFFORT_LEVEL="+effort)
	}
	if token != "" {
		env = append(env, TaskTokenEnv+"="+token)
	}
	return append(env, "CLAUDE_CODE_DISABLE_AUTO_MEMORY=1", "CLAUDE_CODE_RESUME_INTERRUPTED_TURN=0")
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
