//go:build !windows

package executor

import (
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"
)

type processControl struct{}

func configureProcess(cmd *exec.Cmd) (*processControl, error) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return &processControl{}, nil
}
func (*processControl) start(cmd *exec.Cmd) error { return cmd.Start() }
func (*processControl) resume() error             { return nil }
func (*processControl) close()                    {}
func (*processControl) terminate(cmd *exec.Cmd, force bool) {
	if cmd.Process == nil {
		return
	}
	signal := unix.SIGTERM
	if force {
		signal = unix.SIGKILL
	}
	_ = unix.Kill(-cmd.Process.Pid, signal)
}
func exitSignal(state *os.ProcessState) string {
	if status, ok := state.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return status.Signal().String()
	}
	return ""
}
