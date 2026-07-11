package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/boarkshop/boarkshop/internal/event"
	processrun "github.com/boarkshop/boarkshop/internal/process"
	"github.com/boarkshop/boarkshop/internal/storage"
)

type runnerFunc func(context.Context, processrun.Spec) processrun.Result

func (f runnerFunc) Run(ctx context.Context, spec processrun.Spec) processrun.Result {
	return f(ctx, spec)
}

func TestExecutorFanoutIsolationAndFileExchange(t *testing.T) {
	layout, err := storage.Prepare(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}

	pipelineRoot := t.TempDir()
	definitions := []Pipeline{
		pipelineDefinition(t, pipelineRoot, "accepted", "accept", "write", "read"),
		pipelineDefinition(t, pipelineRoot, "accepted-two", "accept"),
		pipelineDefinition(t, pipelineRoot, "rejected", "reject", "must-not-run"),
		pipelineDefinition(t, pipelineRoot, "broken", "error", "must-not-run"),
		pipelineDefinition(t, pipelineRoot, "timed-out", "timeout", "must-not-run"),
	}

	var mu sync.Mutex
	invocations := make([]string, 0)
	runner := runnerFunc(func(_ context.Context, spec processrun.Spec) processrun.Result {
		command := spec.Argv[0]
		mu.Lock()
		invocations = append(invocations, command)
		mu.Unlock()
		switch command {
		case "accept":
			return processrun.Result{ExitCode: 0}
		case "reject":
			return processrun.Result{ExitCode: 1, Err: errors.New("exit status 1")}
		case "error":
			return processrun.Result{ExitCode: 2, Err: errors.New("exit status 2")}
		case "timeout":
			return processrun.Result{ExitCode: -1, TimedOut: true, Err: context.DeadlineExceeded}
		case "write":
			if err := os.WriteFile(filepath.Join(spec.Cwd, "handoff"), []byte("next"), 0o600); err != nil {
				return processrun.Result{ExitCode: -1, Err: err}
			}
			return processrun.Result{ExitCode: 0}
		case "read":
			if got, err := os.ReadFile(filepath.Join(spec.Cwd, "handoff")); err != nil || string(got) != "next" {
				return processrun.Result{ExitCode: -1, Err: errors.New("handoff missing")}
			}
			env := envMap(spec.Env)
			if err := os.WriteFile(filepath.Join(env["BOARKSHOP_DATA_DIR"], "state"), []byte("persistent"), 0o600); err != nil {
				return processrun.Result{ExitCode: -1, Err: err}
			}
			if err := os.WriteFile(filepath.Join(env["BOARKSHOP_SHARED_DIR"], "shared"), []byte("persistent"), 0o600); err != nil {
				return processrun.Result{ExitCode: -1, Err: err}
			}
			return processrun.Result{ExitCode: 0}
		default:
			return processrun.Result{ExitCode: -1, Err: errors.New("unexpected command")}
		}
	})

	executor, err := New(definitions, runner, layout, 2, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	document, err := event.NewWithMetadata("http", 1, strings.Repeat("a", 32), time.Unix(1, 0), map[string]any{"method": "POST"})
	if err != nil {
		t.Fatal(err)
	}
	executor.Handle(context.Background(), *document)

	mu.Lock()
	gotInvocations := strings.Join(invocations, ",")
	acceptCount := 0
	for _, invocation := range invocations {
		if invocation == "accept" {
			acceptCount++
		}
	}
	mu.Unlock()
	if strings.Contains(gotInvocations, "must-not-run") {
		t.Fatalf("a rejected or broken pipeline ran its steps: %s", gotInvocations)
	}
	for _, expected := range []string{"accept", "reject", "error", "timeout", "write", "read"} {
		if !strings.Contains(gotInvocations, expected) {
			t.Errorf("missing invocation %q in %s", expected, gotInvocations)
		}
	}
	if acceptCount != 2 {
		t.Errorf("accepted guard ran %d times, want 2: %s", acceptCount, gotInvocations)
	}

	stateDir, err := layout.PipelineData("accepted")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(stateDir, "state")); err != nil || string(got) != "persistent" {
		t.Fatalf("pipeline state missing: %q, %v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(layout.SharedDir, "shared")); err != nil || string(got) != "persistent" {
		t.Fatalf("shared state missing: %q, %v", got, err)
	}
	entries, err := os.ReadDir(layout.RunsDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("run directories were not cleaned up: %v", entries)
	}
	reopened, err := storage.Prepare(layout.DataDir)
	if err != nil {
		t.Fatalf("prepare layout after simulated restart: %v", err)
	}
	reopenedState, err := reopened.PipelineData("accepted")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(reopenedState, "state")); err != nil || string(got) != "persistent" {
		t.Fatalf("pipeline state did not survive restart: %q, %v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(reopened.SharedDir, "shared")); err != nil || string(got) != "persistent" {
		t.Fatalf("shared state did not survive restart: %q, %v", got, err)
	}
}

func TestExecutorBoundsProcesses(t *testing.T) {
	layout, err := storage.Prepare(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	pipelines := []Pipeline{
		pipelineDefinition(t, root, "one", "block"),
		pipelineDefinition(t, root, "two", "block"),
		pipelineDefinition(t, root, "three", "block"),
	}

	release := make(chan struct{})
	started := make(chan struct{}, len(pipelines))
	var active atomic.Int32
	var maximum atomic.Int32
	runner := runnerFunc(func(ctx context.Context, _ processrun.Spec) processrun.Result {
		current := active.Add(1)
		for {
			old := maximum.Load()
			if current <= old || maximum.CompareAndSwap(old, current) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		active.Add(-1)
		return processrun.Result{ExitCode: 1, Err: errors.New("rejected")}
	})
	executor, err := New(pipelines, runner, layout, 2, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	document := testDocument(t, "b")
	done := make(chan struct{})
	go func() {
		executor.Handle(context.Background(), document)
		close(done)
	}()

	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("two processes did not start")
		}
	}
	select {
	case <-started:
		t.Fatal("third process exceeded the configured limit")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("executor did not finish")
	}
	if got := maximum.Load(); got != 2 {
		t.Fatalf("maximum parallel processes = %d, want 2", got)
	}
}

func TestExecutorSerializesOnePipeline(t *testing.T) {
	layout, err := storage.Prepare(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	pipeline := pipelineDefinition(t, t.TempDir(), "serial", "block")
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	var active atomic.Int32
	var maximum atomic.Int32
	runner := runnerFunc(func(context.Context, processrun.Spec) processrun.Result {
		current := active.Add(1)
		if current > maximum.Load() {
			maximum.Store(current)
		}
		started <- struct{}{}
		<-release
		active.Add(-1)
		return processrun.Result{ExitCode: 1, Err: errors.New("rejected")}
	})
	executor, err := New([]Pipeline{pipeline}, runner, layout, 4, discardLogger())
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for _, marker := range []string{"c", "d"} {
		wg.Add(1)
		go func(marker string) {
			defer wg.Done()
			executor.Handle(context.Background(), testDocument(t, marker))
		}(marker)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first run did not start")
	}
	select {
	case <-started:
		t.Fatal("two runs of one pipeline overlapped")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	wg.Wait()
	if got := maximum.Load(); got != 1 {
		t.Fatalf("maximum parallel runs for one pipeline = %d, want 1", got)
	}
}

func pipelineDefinition(t *testing.T, root, id string, commands ...string) Pipeline {
	t.Helper()
	directory := filepath.Join(root, id)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	definition := Pipeline{
		ID:        id,
		Directory: directory,
		Guard:     Step{ID: "guard", Argv: []string{commands[0]}, Timeout: time.Second},
	}
	for index, command := range commands[1:] {
		definition.Steps = append(definition.Steps, Step{ID: "step-" + command, Argv: []string{command}, Timeout: time.Second})
		_ = index
	}
	return definition
}

func testDocument(t *testing.T, marker string) event.Document {
	t.Helper()
	document, err := event.NewWithMetadata("test", 1, strings.Repeat(marker, 32), time.Unix(1, 0), map[string]any{"marker": marker})
	if err != nil {
		t.Fatal(err)
	}
	return *document
}

func envMap(entries []string) map[string]string {
	result := make(map[string]string, len(entries))
	for _, entry := range entries {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			result[key] = value
		}
	}
	return result
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}
