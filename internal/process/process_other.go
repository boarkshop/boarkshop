//go:build !unix && !windows

package process

import (
	"errors"
	"os"
	"os/exec"
)

func prepareCommand(_ *exec.Cmd) {}

func terminateProcess(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	err := cmd.Process.Kill()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}
