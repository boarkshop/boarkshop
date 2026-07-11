package process

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const helperEnvironmentKey = "GO_WANT_BOARKSHOP_PROCESS_HELPER"

func TestRunnerSuccess(t *testing.T) {
	result := (Runner{}).Run(context.Background(), helperSpec("output", "hello", "warning"))

	if result.Err != nil {
		t.Fatalf("Run() error = %v", result.Err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("Run() exit code = %d, want 0", result.ExitCode)
	}
	if got := string(result.Stdout); got != "hello" {
		t.Fatalf("Run() stdout = %q, want %q", got, "hello")
	}
	if got := string(result.Stderr); got != "warning" {
		t.Fatalf("Run() stderr = %q, want %q", got, "warning")
	}
	if result.TimedOut || result.Canceled {
		t.Fatalf("Run() context flags = timed_out:%t canceled:%t", result.TimedOut, result.Canceled)
	}
}

func TestRunnerNonZeroExit(t *testing.T) {
	result := (Runner{}).Run(context.Background(), helperSpec("exit", "23"))

	if result.ExitCode != 23 {
		t.Fatalf("Run() exit code = %d, want 23", result.ExitCode)
	}
	var exitErr *exec.ExitError
	if !errors.As(result.Err, &exitErr) {
		t.Fatalf("Run() error = %T %v, want *exec.ExitError", result.Err, result.Err)
	}
}

func TestRunnerSetsWorkingDirectoryAndEnvironment(t *testing.T) {
	dir := t.TempDir()
	spec := helperSpec("inspect", "BOARKSHOP_PROCESS_TEST_VALUE")
	spec.Cwd = dir
	spec.Env = append(spec.Env, "BOARKSHOP_PROCESS_TEST_VALUE=present")

	result := (Runner{}).Run(context.Background(), spec)
	if result.Err != nil {
		t.Fatalf("Run() error = %v; stderr = %q", result.Err, result.Stderr)
	}

	parts := strings.SplitN(string(result.Stdout), "\n", 2)
	if len(parts) != 2 {
		t.Fatalf("Run() stdout = %q, want cwd and environment value", result.Stdout)
	}
	if got := filepath.Clean(parts[0]); got != filepath.Clean(dir) {
		t.Fatalf("Run() cwd = %q, want %q", got, dir)
	}
	if parts[1] != "present" {
		t.Fatalf("Run() environment value = %q, want %q", parts[1], "present")
	}
}

func TestRunnerCapsOutputIndependently(t *testing.T) {
	const limit = 17
	result := (Runner{OutputLimit: limit}).Run(
		context.Background(),
		helperSpec("spam", "100", "80"),
	)

	if result.Err != nil {
		t.Fatalf("Run() error = %v", result.Err)
	}
	if len(result.Stdout) != limit || string(result.Stdout) != strings.Repeat("o", limit) {
		t.Fatalf("Run() stdout = %q (%d bytes), want %d bytes", result.Stdout, len(result.Stdout), limit)
	}
	if len(result.Stderr) != limit || string(result.Stderr) != strings.Repeat("e", limit) {
		t.Fatalf("Run() stderr = %q (%d bytes), want %d bytes", result.Stderr, len(result.Stderr), limit)
	}
	if !result.StdoutTruncated || !result.StderrTruncated {
		t.Fatalf("Run() truncated flags = stdout:%t stderr:%t, want both true", result.StdoutTruncated, result.StderrTruncated)
	}
}

func TestRunnerTimesOutAndTerminatesProcess(t *testing.T) {
	spec := helperSpec("sleep", "5s")
	spec.Timeout = 150 * time.Millisecond

	result := (Runner{}).Run(context.Background(), spec)

	if !result.TimedOut || result.Canceled {
		t.Fatalf("Run() context flags = timed_out:%t canceled:%t", result.TimedOut, result.Canceled)
	}
	if !errors.Is(result.Err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want context deadline exceeded", result.Err)
	}
	if result.Duration >= 4*time.Second {
		t.Fatalf("Run() duration = %v, process was not terminated promptly", result.Duration)
	}
}

func TestRunnerCancellationTerminatesProcess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	timer := time.AfterFunc(150*time.Millisecond, cancel)
	defer timer.Stop()

	result := (Runner{}).Run(ctx, helperSpec("sleep", "5s"))

	if result.TimedOut || !result.Canceled {
		t.Fatalf("Run() context flags = timed_out:%t canceled:%t", result.TimedOut, result.Canceled)
	}
	if !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context canceled", result.Err)
	}
	if result.Duration >= 4*time.Second {
		t.Fatalf("Run() duration = %v, process was not terminated promptly", result.Duration)
	}
}

func TestRunnerDoesNotStartWithCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result := (Runner{}).Run(ctx, Spec{Argv: []string{"executable-that-does-not-exist"}})

	if !result.Canceled || result.TimedOut {
		t.Fatalf("Run() context flags = timed_out:%t canceled:%t", result.TimedOut, result.Canceled)
	}
	if !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context canceled", result.Err)
	}
	if result.ExitCode != ExitCodeNotStarted {
		t.Fatalf("Run() exit code = %d, want %d", result.ExitCode, ExitCodeNotStarted)
	}
}

func TestRunnerRejectsInvalidSpecs(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		spec Spec
		want error
	}{
		{name: "nil context", spec: Spec{Argv: []string{"unused"}}, want: ErrNilContext},
		{name: "empty argv", ctx: context.Background(), spec: Spec{}, want: ErrEmptyArgv},
		{name: "empty executable", ctx: context.Background(), spec: Spec{Argv: []string{""}}, want: ErrEmptyArgv},
		{name: "negative timeout", ctx: context.Background(), spec: Spec{Argv: []string{"unused"}, Timeout: -time.Second}, want: ErrInvalidTimeout},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := (Runner{}).Run(test.ctx, test.spec)
			if !errors.Is(result.Err, test.want) {
				t.Fatalf("Run() error = %v, want %v", result.Err, test.want)
			}
			if result.ExitCode != ExitCodeNotStarted {
				t.Fatalf("Run() exit code = %d, want %d", result.ExitCode, ExitCodeNotStarted)
			}
		})
	}
}

func TestRunnerDoesNotInvokeShell(t *testing.T) {
	result := (Runner{}).Run(context.Background(), Spec{
		Argv: []string{"definitely-not-an-executable; echo shell-was-used"},
	})

	if result.Err == nil {
		t.Fatal("Run() error = nil, want executable lookup failure")
	}
	if len(result.Stdout) != 0 {
		t.Fatalf("Run() stdout = %q, want empty output", result.Stdout)
	}
}

func helperSpec(args ...string) Spec {
	argv := []string{os.Args[0], "-test.run=^TestProcessHelper$", "--"}
	argv = append(argv, args...)
	env := append([]string(nil), os.Environ()...)
	env = append(env, helperEnvironmentKey+"=1")
	return Spec{Argv: argv, Env: env}
}

func TestProcessHelper(t *testing.T) {
	if os.Getenv(helperEnvironmentKey) != "1" {
		return
	}

	separator := -1
	for i, arg := range os.Args {
		if arg == "--" {
			separator = i
			break
		}
	}
	if separator < 0 || separator+1 >= len(os.Args) {
		_, _ = fmt.Fprint(os.Stderr, "missing helper command")
		os.Exit(2)
	}
	args := os.Args[separator+1:]

	switch args[0] {
	case "output":
		_, _ = io.WriteString(os.Stdout, args[1])
		_, _ = io.WriteString(os.Stderr, args[2])
		os.Exit(0)
	case "exit":
		code, err := strconv.Atoi(args[1])
		if err != nil {
			os.Exit(2)
		}
		os.Exit(code)
	case "inspect":
		cwd, err := os.Getwd()
		if err != nil {
			os.Exit(2)
		}
		_, _ = fmt.Fprintf(os.Stdout, "%s\n%s", cwd, os.Getenv(args[1]))
		os.Exit(0)
	case "spam":
		stdoutBytes, err := strconv.Atoi(args[1])
		if err != nil {
			os.Exit(2)
		}
		stderrBytes, err := strconv.Atoi(args[2])
		if err != nil {
			os.Exit(2)
		}
		_, _ = io.WriteString(os.Stdout, strings.Repeat("o", stdoutBytes))
		_, _ = io.WriteString(os.Stderr, strings.Repeat("e", stderrBytes))
		os.Exit(0)
	case "sleep":
		duration, err := time.ParseDuration(args[1])
		if err != nil {
			os.Exit(2)
		}
		time.Sleep(duration)
		os.Exit(0)
	default:
		_, _ = fmt.Fprintf(os.Stderr, "unknown helper command %q", args[0])
		os.Exit(2)
	}
}
