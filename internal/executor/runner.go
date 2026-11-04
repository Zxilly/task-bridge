// Package executor runs bounded Bash commands in a configured local workspace.
package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Zxilly/task-bridge/internal/processcontrol"
)

type Config struct {
	BashPath       string
	Root           string
	Env            []string
	MaxOutputBytes int64
	KillGrace      time.Duration
	Backgrounded   func(shellExitCode int) error
}

type Request struct {
	Command    string
	Cwd        string
	Timeout    time.Duration
	Background bool
}

type Chunk struct {
	Sequence int64  `json:"sequence"`
	Stream   string `json:"stream"`
	Data     []byte `json:"data"`
}

type Result struct {
	State            string `json:"state"`
	ExitCode         int    `json:"exit_code"`
	Signal           string `json:"signal,omitempty"`
	Error            string `json:"error,omitempty"`
	Cwd              string `json:"cwd"`
	DurationMS       int64  `json:"duration_ms"`
	Truncated        bool   `json:"truncated"`
	DroppedBytes     int64  `json:"dropped_bytes"`
	OutputIncomplete bool   `json:"output_incomplete,omitempty"`
	Background       bool   `json:"background,omitempty"`
	ShellExitCode    *int   `json:"shell_exit_code,omitempty"`
}

// ResolveCwd checks real paths, including symlinks. This is routing protection,
// not a sandbox: Bash retains the privileges of the user running the client.
func ResolveCwd(root, requested string) (string, error) {
	base, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	base, err = filepath.EvalSymlinks(base)
	if err != nil {
		return "", fmt.Errorf("workspace root: %w", err)
	}
	dir := requested
	if dir == "" {
		dir = base
	} else if !filepath.IsAbs(dir) {
		dir = filepath.Join(base, dir)
	}
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		return "", fmt.Errorf("working directory: %w", err)
	}
	rel, err := filepath.Rel(base, dir)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", errors.New("working directory is outside the configured workspace")
	}
	info, err := os.Stat(dir)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", errors.New("working directory is not a directory")
	}
	return dir, nil
}

// Run does not inherit stdin/stdout. emit must durably store each chunk before
// returning. An emit failure cancels execution; pipes are still drained.
func Run(ctx context.Context, cfg Config, req Request, started func(int) error, emit func(Chunk) error) Result {
	begin := time.Now()
	result := Result{State: "failed", ExitCode: -1}
	dir, err := ResolveCwd(cfg.Root, req.Cwd)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	result.Cwd = dir
	if req.Command == "" || len(req.Command) > 128*1024 {
		result.Error = "command must contain 1 to 131072 bytes"
		return result
	}
	if req.Timeout <= 0 {
		req.Timeout = 120 * time.Second
	}
	if req.Timeout > 15*time.Minute {
		result.Error = "command timeout exceeds 15 minutes"
		return result
	}
	if cfg.MaxOutputBytes <= 0 {
		cfg.MaxOutputBytes = 10 * 1024 * 1024
	}
	if cfg.KillGrace <= 0 {
		cfg.KillGrace = 2 * time.Second
	}
	commandCtx, cancel := context.WithTimeout(ctx, req.Timeout)
	defer cancel()
	if err := commandCtx.Err(); err != nil {
		result.State = "cancelled"
		result.Error = err.Error()
		return result
	}
	cmd := exec.Command(cfg.BashPath, "--noprofile", "--norc", "-c", req.Command)
	cmd.Dir = dir
	cmd.Env = cfg.Env
	if cmd.Env == nil {
		cmd.Env = DefaultEnv()
	}
	process, err := processcontrol.Prepare(cmd)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer process.Close()
	output := &collector{max: cfg.MaxOutputBytes, emit: emit, cancel: cancel}
	// Own the pipes so cmd.Wait reports the shell's exit independently of EOF
	// from descendants which inherited stdout/stderr (as in Codex exec).
	capture, err := newCommandOutput(cmd, output)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer capture.close()
	if err = process.Start(cmd); err != nil {
		result.Error = err.Error()
		return result
	}
	capture.closeWriters()
	if started != nil {
		if err = started(cmd.Process.Pid); err != nil {
			output.setError(err)
			cancel()
		}
	}
	if commandCtx.Err() == nil {
		if err = process.Resume(); err != nil {
			process.Terminate(cmd, true)
			_ = cmd.Wait()
			result.Error = err.Error()
			return result
		}
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var interrupted bool
	select {
	case err = <-done:
	case <-commandCtx.Done():
		interrupted = true
		process.Terminate(cmd, false)
		timer := time.NewTimer(cfg.KillGrace)
		select {
		case err = <-done:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			process.Terminate(cmd, true)
			err = <-done
		}
		// A shell can exit on SIGTERM while an ordinary descendant ignores it.
		// Finish cancelling the entire process group/job in that case too.
		process.Terminate(cmd, true)
	}
	result.Background = req.Background
	if !interrupted && cmd.ProcessState != nil {
		shellCode := cmd.ProcessState.ExitCode()
		result.ShellExitCode = &shellCode
		if !capture.wait(2 * time.Second) {
			result.Background = true
			if cfg.Backgrounded != nil {
				if notifyErr := cfg.Backgrounded(shellCode); notifyErr != nil {
					output.setError(notifyErr)
					cancel()
				}
			}
			select {
			case <-capture.done:
			case <-commandCtx.Done():
				interrupted = true
				process.Terminate(cmd, true)
			}
		}
	}
	result.OutputIncomplete = capture.drain(2 * time.Second)
	result.DurationMS = time.Since(begin).Milliseconds()
	if cmd.ProcessState != nil {
		result.ExitCode = cmd.ProcessState.ExitCode()
		result.Signal = processcontrol.ExitSignal(cmd.ProcessState)
	}
	result.Truncated = output.dropped > 0
	result.DroppedBytes = output.dropped
	switch {
	case output.err != nil:
		result.State = "failed"
		result.Error = "output journal: " + output.err.Error()
	case interrupted && errors.Is(commandCtx.Err(), context.DeadlineExceeded):
		result.State = "timed_out"
	case interrupted && ctx.Err() != nil:
		result.State = "cancelled"
	default:
		result.State = "exited"
		if err != nil {
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				result.State = "failed"
				result.Error = err.Error()
			}
		}
	}
	return result
}

// DefaultEnv is deliberately narrow; token variables and unrelated secrets are
// not inherited by commands. Additional variables must be configured locally.
func DefaultEnv() []string {
	keys := []string{"PATH", "HOME", "USER", "LOGNAME", "LANG", "LC_ALL", "TMPDIR", "TMP", "TEMP", "SystemRoot", "SYSTEMROOT", "COMSPEC", "PATHEXT", "USERPROFILE", "HOMEDRIVE", "HOMEPATH"}
	var env []string
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+value)
		}
	}
	return env
}

type collector struct {
	mu                      sync.Mutex
	max, kept, dropped, seq int64
	emit                    func(Chunk) error
	cancel                  context.CancelFunc
	err                     error
}

func (c *collector) setError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		c.err = err
	}
}

type streamWriter struct {
	c      *collector
	stream string
}

func (w streamWriter) Write(p []byte) (int, error) {
	n := len(p)
	c := w.c
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(p) > 0 {
		size := len(p)
		if size > 32*1024 {
			size = 32 * 1024
		}
		part := p[:size]
		p = p[size:]
		available := c.max - c.kept
		if int64(len(part)) > available {
			c.dropped += int64(len(part)) - available
			part = part[:int(available)]
		}
		if len(part) == 0 {
			continue
		}
		c.kept += int64(len(part))
		c.seq++
		if c.emit != nil && c.err == nil {
			if err := c.emit(Chunk{Sequence: c.seq, Stream: w.stream, Data: append([]byte(nil), part...)}); err != nil {
				c.err = err
				c.cancel()
			}
		}
	}
	return n, nil
}

var _ io.Writer = streamWriter{}
