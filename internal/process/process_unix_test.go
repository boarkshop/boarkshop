//go:build unix

package process

import (
	"os"
	"os/exec"
	"testing"
)

func TestPrepareCommandCreatesProcessGroup(t *testing.T) {
	cmd := exec.Command(os.Args[0])
	prepareCommand(cmd)

	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		t.Fatal("prepareCommand() did not configure a separate process group")
	}
}
