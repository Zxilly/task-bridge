//go:build !windows

package processcontrol

import (
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"
)

type Control struct{}

func Prepare(cmd *exec.Cmd) (*Control, error) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return &Control{}, nil
}
func (*Control) Start(cmd *exec.Cmd) error { return cmd.Start() }
func (*Control) Resume() error             { return nil }
func (*Control) Close()                    {}
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
