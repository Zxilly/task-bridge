package processcontrol

import "os/exec"

// PrepareForParentExit uses the ordinary kill-on-close job, whose handle is
// released by Windows even if the owning worker is forcibly terminated.
func PrepareForParentExit(cmd *exec.Cmd) (*Control, error) {
	return Prepare(cmd)
}
