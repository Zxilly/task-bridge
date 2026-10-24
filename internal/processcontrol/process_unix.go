//go:build !windows

package processcontrol

import (
	"os"
	"os/exec"
	"runtime"
	"syscall"

	"golang.org/x/sys/unix"
)

type Control struct {
	parentExit   bool
	threadPinned bool
}

func Prepare(cmd *exec.Cmd) (*Control, error) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return &Control{}, nil
}
func (p *Control) Start(cmd *exec.Cmd) error {
	if p.parentExit && !p.threadPinned {
		// Linux parent-death signals follow the creating OS thread. Keep that
		// thread alive until the caller has reaped the CLI and closes control.
		runtime.LockOSThread()
		p.threadPinned = true
	}
	return cmd.Start()
}
func (*Control) Resume() error { return nil }
func (p *Control) Close() {
	if p.threadPinned {
		p.threadPinned = false
		runtime.UnlockOSThread()
	}
}
func (*Control) Terminate(cmd *exec.Cmd, force bool) {
	if cmd.Process == nil {
		return
	}
	signal := unix.SIGTERM
	if force {
		signal = unix.SIGKILL
	}
	_ = unix.Kill(-cmd.Process.Pid, signal)
}
func ExitSignal(state *os.ProcessState) string {
	if status, ok := state.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return status.Signal().String()
	}
	return ""
}
