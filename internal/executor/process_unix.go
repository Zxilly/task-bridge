//go:build !windows

package executor

import (
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"
)

func configureProcess(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }
func terminateTree(cmd *exec.Cmd, force bool) {
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
