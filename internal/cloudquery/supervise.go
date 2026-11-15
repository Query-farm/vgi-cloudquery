// Keeping plugin processes from outliving the worker.
//
// DuckDB may stop a worker abruptly (SIGKILL, or a stdio worker that exits as
// soon as its input closes), and only Linux can ask the kernel to kill a child
// when its parent dies. So the worker starts each plugin through a tiny
// supervisor: the worker binary itself, re-executed as
//
//	vgi-cloudquery-worker __cq_supervise <plugin> <args...>
//
// with a pipe on its stdin. The supervisor starts the plugin and kills it as
// soon as that pipe closes, which happens however the worker ends.

package cloudquery

import (
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
)

// SuperviseArg is the hidden first argument that runs the supervisor.
const SuperviseArg = "__cq_supervise"

// supervisorBinary is the executable re-run as a supervisor; empty starts
// plugins directly (unit tests, where StopPlugins cleans up).
var supervisorBinary string

// EnableSupervisor makes plugins start under the given executable's
// supervisor mode (normally os.Executable() of the worker).
func EnableSupervisor(executable string) { supervisorBinary = executable }

// Supervise runs the supervisor: start argv, kill it when stdin closes or a
// termination signal arrives, and exit with its status.
func Supervise(argv []string) int {
	if len(argv) == 0 {
		return 2
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	setPdeathsig(cmd)
	if err := cmd.Start(); err != nil {
		_, _ = os.Stderr.WriteString("cloudquery plugin supervisor: " + err.Error() + "\n")
		return 1
	}
	kill := func() { _ = cmd.Process.Kill() }
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin) // returns when the worker closes the pipe or dies
		kill()
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		kill()
	}()
	if err := cmd.Wait(); err != nil {
		if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() >= 0 {
			return ee.ExitCode()
		}
		return 1
	}
	return 0
}
