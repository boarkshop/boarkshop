//go:build unix

package process

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

func prepareCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func terminateProcess(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}

	groupErr := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	if groupErr == nil || errors.Is(groupErr, syscall.ESRCH) {
		return nil
	}

	processErr := cmd.Process.Kill()
	if processErr == nil || errors.Is(processErr, os.ErrProcessDone) {
		return nil
	}
	return fmt.Errorf("kill process group: %v; kill process: %w", groupErr, processErr)
}
