//go:build !windows && !linux

package processcontrol

import (
	"errors"
	"os/exec"
)

func PrepareForParentExit(*exec.Cmd) (*Control, error) {
	return nil, errors.New("Claude session ownership after worker exit requires Windows or Linux")
}
