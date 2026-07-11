// Package process executes pipeline commands without implicitly invoking a
// shell.
package process

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

const (
	// DefaultOutputLimit is the maximum number of bytes retained independently
	// for stdout and stderr by a zero-value Runner.
	DefaultOutputLimit = 64 * 1024

	// ExitCodeNotStarted is returned when a process did not start or did not
	// expose a regular exit code (for example, after being killed by a signal).
	ExitCodeNotStarted = -1
)

var (
	ErrNilContext     = errors.New("process: nil context")
	ErrEmptyArgv      = errors.New("process: argv must contain an executable")
	ErrInvalidTimeout = errors.New("process: timeout must not be negative")
)

// Spec describes one process invocation. Argv is passed directly to the
// operating system; Runner never adds a shell. Cwd may be empty to inherit the
// current working directory. A nil Env inherits the parent environment, while
// a non-nil Env is used as the complete environment of the child process.
// A zero Timeout disables the per-command timeout.
type Spec struct {
	Argv    []string
	Cwd     string
	Env     []string
	Timeout time.Duration
}

// Result describes a completed invocation, including failures to start and
// context-triggered termination. ExitCode is ExitCodeNotStarted when no
// regular exit code is available.
type Result struct {
	ExitCode int
	Duration time.Duration

	TimedOut bool
	Canceled bool

	Stdout          []byte
	Stderr          []byte
	StdoutTruncated bool
	StderrTruncated bool

	Err error
}

// Runner executes commands. Its zero value is ready to use. OutputLimit is
// applied independently to stdout and stderr; values less than or equal to
// zero select DefaultOutputLimit.
type Runner struct {
	OutputLimit int
}

// Run executes spec and waits for the process to finish. On context
// cancellation or deadline expiry it terminates the process and, on Unix,
// attempts to terminate its process group as well.
func (r Runner) Run(ctx context.Context, spec Spec) (result Result) {
	startedAt := time.Now()
	result.ExitCode = ExitCodeNotStarted

	limit := r.OutputLimit
	if limit <= 0 {
		limit = DefaultOutputLimit
	}
	stdout := newCappedBuffer(limit)
	stderr := newCappedBuffer(limit)
	defer func() {
		result.Duration = time.Since(startedAt)
		result.Stdout = stdout.Bytes()
		result.Stderr = stderr.Bytes()
		result.StdoutTruncated = stdout.Truncated()
		result.StderrTruncated = stderr.Truncated()
	}()

	if ctx == nil {
		result.Err = ErrNilContext
		return result
	}
	if len(spec.Argv) == 0 || spec.Argv[0] == "" {
		result.Err = ErrEmptyArgv
		return result
	}
	if spec.Timeout < 0 {
		result.Err = ErrInvalidTimeout
		return result
	}

	runCtx := ctx
	cancel := func() {}
	if spec.Timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, spec.Timeout)
	}
	defer cancel()

	if err := runCtx.Err(); err != nil {
		setContextResult(&result, err)
		return result
	}

	argv := append([]string(nil), spec.Argv...)
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = spec.Cwd
	if spec.Env != nil {
		cmd.Env = append([]string(nil), spec.Env...)
	}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	prepareCommand(cmd)

	if err := cmd.Start(); err != nil {
		result.Err = fmt.Errorf("start %q: %w", argv[0], err)
		return result
	}

	waited := make(chan error, 1)
	go func() {
		waited <- cmd.Wait()
	}()

	var waitErr error
	select {
	case waitErr = <-waited:
		// The process completed without intervention.
	case <-runCtx.Done():
		// Prefer a process result that became available at the same time as the
		// context signal. This avoids reporting a timeout for an already reaped
		// process at a tight deadline boundary.
		select {
		case waitErr = <-waited:
		default:
			contextErr := runCtx.Err()
			terminateErr := terminateProcess(cmd)
			waitErr = <-waited
			setContextResult(&result, contextErr)
			if terminateErr != nil {
				result.Err = fmt.Errorf("%w (terminate process: %v)", contextErr, terminateErr)
			}
		}
	}

	result.ExitCode = exitCode(waitErr)
	if result.Err == nil {
		result.Err = waitErr
	}
	return result
}

func setContextResult(result *Result, err error) {
	result.Err = err
	result.TimedOut = errors.Is(err, context.DeadlineExceeded)
	result.Canceled = errors.Is(err, context.Canceled)
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return ExitCodeNotStarted
}
