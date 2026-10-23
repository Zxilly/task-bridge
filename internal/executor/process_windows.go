//go:build windows

package executor

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"

	"golang.org/x/sys/windows"
)

func configureProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: windows.CREATE_NEW_PROCESS_GROUP}
}
func terminateTree(cmd *exec.Cmd, force bool) {
	if cmd.Process == nil {
		return
	}
	// taskkill is Windows' supported process-tree termination mechanism. Resolve
	// it under SystemRoot rather than through a workspace-controlled PATH.
	root := os.Getenv("SystemRoot")
	if root == "" {
		root = `C:\Windows`
	}
	kill := exec.Command(filepath.Join(root, "System32", "taskkill.exe"), "/PID", strconv.Itoa(cmd.Process.Pid), "/T", "/F")
	kill.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	_ = kill.Run()
	_ = cmd.Process.Kill()
}
func exitSignal(state *os.ProcessState) string { return "" }
