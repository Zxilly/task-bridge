package processcontrol

import (
	"os/exec"
	"syscall"
)

// PrepareForParentExit prepares a CLI process owned by its launching goroutine.
// Start and Close must be called on that same goroutine.
func PrepareForParentExit(cmd *exec.Cmd) (*Control, error) {
	control, err := Prepare(cmd)
	if err != nil {
		return nil, err
	}
	cmd.SysProcAttr.Pdeathsig = syscall.SIGKILL
	control.parentExit = true
	return control, nil
}
