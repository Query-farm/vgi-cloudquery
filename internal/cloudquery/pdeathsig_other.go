//go:build !linux

package cloudquery

import "os/exec"

// setPdeathsig is Linux-only; elsewhere StopPlugins and the idle reaper
// stop plugin processes.
func setPdeathsig(*exec.Cmd) {}
