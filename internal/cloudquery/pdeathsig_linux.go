package cloudquery

import (
	"os/exec"
	"syscall"
)

// setPdeathsig kills the plugin if the worker dies without cleaning up.
func setPdeathsig(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
}
