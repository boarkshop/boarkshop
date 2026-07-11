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

type mutablePipelineSource struct {
	mu          sync.Mutex
	definitions []Pipeline
	err         error
	revision    int
	loaded      chan int
}

type blockingPipelineSource struct {
	started  chan struct{}
	release  chan struct{}
	finished chan struct{}
}

func (source *blockingPipelineSource) Load() ([]Pipeline, error) {
	defer close(source.finished)
	close(source.started)
	<-source.release
	return nil, nil
}

func newMutablePipelineSource(definitions []Pipeline) *mutablePipelineSource {
	return &mutablePipelineSource{
		definitions: append([]Pipeline(nil), definitions...),
		revision:    1,
		loaded:      make(chan int, 64),
	}
}

func (source *mutablePipelineSource) Load() ([]Pipeline, error) {
	source.mu.Lock()
	definitions := append([]Pipeline(nil), source.definitions...)
	err := source.err
	revision := source.revision
	source.mu.Unlock()

	source.loaded <- revision
	return definitions, err
}

func (source *mutablePipelineSource) set(definitions []Pipeline, err error) int {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.definitions = append([]Pipeline(nil), definitions...)
	source.err = err
	source.revision++
	return source.revision
}

func (source *mutablePipelineSource) waitForLoad(t *testing.T, revision int) {
	t.Helper()
	for {
		select {
		case loadedRevision := <-source.loaded:
			if loadedRevision >= revision {
				return
			}
		case <-time.After(time.Second):
			t.Fatalf("pipeline source revision %d was not loaded", revision)
		}
	}
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

func TestExecutorHotAddsPipelineOnHandle(t *testing.T) {
	layout, err := storage.Prepare(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	source := newMutablePipelineSource(nil)

	var mu sync.Mutex
	var invocations []string
	runner := runnerFunc(func(_ context.Context, spec processrun.Spec) processrun.Result {
		mu.Lock()
		invocations = append(invocations, spec.Argv[0])
		mu.Unlock()
		return processrun.Result{ExitCode: 1, Err: errors.New("rejected")}
	})
	executor, err := NewDynamic(nil, source, runner, layout, 2, discardLogger())
	if err != nil {
		t.Fatal(err)
	}

	executor.Handle(context.Background(), testDocument(t, "a"))
	mu.Lock()
	if len(invocations) != 0 {
		t.Fatalf("empty snapshot invoked commands: %v", invocations)
	}
	mu.Unlock()

	hot := pipelineDefinition(t, t.TempDir(), "hot", "hot-guard")
	source.set([]Pipeline{hot}, nil)
	executor.Handle(context.Background(), testDocument(t, "b"))

	mu.Lock()
	defer mu.Unlock()
	if len(invocations) != 1 || invocations[0] != "hot-guard" {
		t.Fatalf("invocations after hot add = %v, want [hot-guard]", invocations)
	}
}

func TestExecutorCancellationDoesNotWaitForBlockedReload(t *testing.T) {
	layout, err := storage.Prepare(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	source := &blockingPipelineSource{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		finished: make(chan struct{}),
	}
	executor, err := NewDynamic(nil, source, runnerFunc(func(context.Context, processrun.Spec) processrun.Result {
		t.Fatal("an empty snapshot must not invoke the runner")
		return processrun.Result{}
	}), layout, 1, discardLogger())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		executor.Handle(ctx, testDocument(t, "a"))
		close(done)
	}()
	select {
	case <-source.started:
	case <-time.After(time.Second):
		t.Fatal("pipeline reload did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Handle waited for a blocked reload after cancellation")
	}
	close(source.release)
	select {
	case <-source.finished:
	case <-time.After(time.Second):
		t.Fatal("pipeline reload did not finish after release")
	}
}

func TestExecutorReclaimsIdlePipelineGate(t *testing.T) {
	layout, err := storage.Prepare(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	pipeline := pipelineDefinition(t, t.TempDir(), "reclaimed", "reject")
	executor, err := New([]Pipeline{pipeline}, runnerFunc(func(context.Context, processrun.Spec) processrun.Result {
		return processrun.Result{ExitCode: 1, Err: errors.New("rejected")}
	}), layout, 1, discardLogger())
	if err != nil {
		t.Fatal(err)
	}

	executor.Handle(context.Background(), testDocument(t, "b"))
	executor.gatesMu.Lock()
	gateCount := len(executor.gates)
	executor.gatesMu.Unlock()
	if gateCount != 0 {
		t.Fatalf("idle pipeline gates = %d, want 0", gateCount)
	}
}

func TestExecutorRefreshFailureKeepsWholeLastGoodSnapshot(t *testing.T) {
	layout, err := storage.Prepare(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	initial := []Pipeline{
		pipelineDefinition(t, root, "old-a", "old-a"),
		pipelineDefinition(t, root, "old-b", "old-b"),
	}
	source := newMutablePipelineSource(initial)

	var mu sync.Mutex
	invocations := make(map[string][]string)
	runner := runnerFunc(func(_ context.Context, spec processrun.Spec) processrun.Result {
		eventID := envMap(spec.Env)["BOARKSHOP_EVENT_ID"]
		mu.Lock()
		invocations[eventID] = append(invocations[eventID], spec.Argv[0])
		mu.Unlock()
		return processrun.Result{ExitCode: 1, Err: errors.New("rejected")}
	})
	executor, err := NewDynamic(initial, source, runner, layout, 4, discardLogger())
	if err != nil {
		t.Fatal(err)
	}

	partial := pipelineDefinition(t, root, "partial-new", "partial-new")
	source.set([]Pipeline{partial}, errors.New("manifest is incomplete"))
	failedRefreshEvent := testDocument(t, "c")
	executor.Handle(context.Background(), failedRefreshEvent)

	mu.Lock()
	failedRefreshCalls := append([]string(nil), invocations[failedRefreshEvent.EventID]...)
	mu.Unlock()
	assertCommandCounts(t, failedRefreshCalls, map[string]int{"old-a": 1, "old-b": 1})

	recovered := pipelineDefinition(t, root, "recovered", "recovered")
	source.set([]Pipeline{recovered}, nil)
	recoveredEvent := testDocument(t, "d")
	executor.Handle(context.Background(), recoveredEvent)

	mu.Lock()
	recoveredCalls := append([]string(nil), invocations[recoveredEvent.EventID]...)
	mu.Unlock()
	assertCommandCounts(t, recoveredCalls, map[string]int{"recovered": 1})
}

func TestExecutorUsesOneSnapshotForWholeEvent(t *testing.T) {
	layout, err := storage.Prepare(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	v1 := pipelineDefinition(t, root, "snapshot", "guard-v1", "step-v1")
	v2 := pipelineDefinition(t, root, "snapshot", "guard-v2", "step-v2")
	source := newMutablePipelineSource([]Pipeline{v1})

	firstGuardStarted := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFirst) }) }
	defer release()

	var mu sync.Mutex
	invocations := make(map[string][]string)
	runner := runnerFunc(func(ctx context.Context, spec processrun.Spec) processrun.Result {
		command := spec.Argv[0]
		eventID := envMap(spec.Env)["BOARKSHOP_EVENT_ID"]
		mu.Lock()
		invocations[eventID] = append(invocations[eventID], command)
		mu.Unlock()
		if command == "guard-v1" {
			select {
			case firstGuardStarted <- struct{}{}:
			default:
			}
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return processrun.Result{ExitCode: -1, Canceled: true, Err: ctx.Err()}
			}
		}
		return processrun.Result{ExitCode: 0}
	})
	executor, err := NewDynamic([]Pipeline{v1}, source, runner, layout, 4, discardLogger())
	if err != nil {
		t.Fatal(err)
	}

	firstEvent := testDocument(t, "a")
	firstDone := make(chan struct{})
	go func() {
		executor.Handle(context.Background(), firstEvent)
		close(firstDone)
	}()
	select {
	case <-firstGuardStarted:
	case <-time.After(time.Second):
		t.Fatal("v1 guard did not start")
	}

	revision := source.set([]Pipeline{v2}, nil)
	secondEvent := testDocument(t, "b")
	secondDone := make(chan struct{})
	go func() {
		executor.Handle(context.Background(), secondEvent)
		close(secondDone)
	}()
	source.waitForLoad(t, revision)
	release()

	for name, done := range map[string]<-chan struct{}{"first event": firstDone, "second event": secondDone} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s did not finish", name)
		}
	}

	mu.Lock()
	firstCalls := append([]string(nil), invocations[firstEvent.EventID]...)
	secondCalls := append([]string(nil), invocations[secondEvent.EventID]...)
	mu.Unlock()
	assertCommands(t, firstCalls, "guard-v1", "step-v1")
	assertCommands(t, secondCalls, "guard-v2", "step-v2")
}

func TestExecutorSerializesPipelineAcrossRemovalAndReadd(t *testing.T) {
	layout, err := storage.Prepare(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	v1 := pipelineDefinition(t, root, "serial-reload", "v1-block")
	v2 := pipelineDefinition(t, root, "serial-reload", "v2-run")
	source := newMutablePipelineSource([]Pipeline{v1})

	releaseV1 := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseV1) }) }
	defer release()
	v1Started := make(chan struct{}, 1)
	v2Started := make(chan struct{}, 1)
	var active atomic.Int32
	var maximum atomic.Int32
	runner := runnerFunc(func(ctx context.Context, spec processrun.Spec) processrun.Result {
		current := active.Add(1)
		for {
			old := maximum.Load()
			if current <= old || maximum.CompareAndSwap(old, current) {
				break
			}
		}
		defer active.Add(-1)

		switch spec.Argv[0] {
		case "v1-block":
			select {
			case v1Started <- struct{}{}:
			default:
			}
			select {
			case <-releaseV1:
			case <-ctx.Done():
			}
		case "v2-run":
			select {
			case v2Started <- struct{}{}:
			default:
			}
		}
		return processrun.Result{ExitCode: 1, Err: errors.New("rejected")}
	})
	executor, err := NewDynamic([]Pipeline{v1}, source, runner, layout, 4, discardLogger())
	if err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan struct{})
	go func() {
		executor.Handle(context.Background(), testDocument(t, "a"))
		close(firstDone)
	}()
	select {
	case <-v1Started:
	case <-time.After(time.Second):
		t.Fatal("v1 run did not start")
	}

	source.set(nil, nil)
	removedDone := make(chan struct{})
	go func() {
		executor.Handle(context.Background(), testDocument(t, "b"))
		close(removedDone)
	}()
	select {
	case <-removedDone:
	case <-time.After(time.Second):
		t.Fatal("empty snapshot did not finish while the removed pipeline was still running")
	}

	revision := source.set([]Pipeline{v2}, nil)
	secondDone := make(chan struct{})
	go func() {
		executor.Handle(context.Background(), testDocument(t, "c"))
		close(secondDone)
	}()
	source.waitForLoad(t, revision)
	waitForGateRefs(t, executor, "serial-reload", 2)
	select {
	case <-v2Started:
		t.Fatal("re-added pipeline overlapped the previous run of the same id")
	default:
	}

	release()
	for name, done := range map[string]<-chan struct{}{"v1 run": firstDone, "v2 run": secondDone} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s did not finish", name)
		}
	}
	if got := maximum.Load(); got != 1 {
		t.Fatalf("maximum parallel runs for one id across reload = %d, want 1", got)
	}
}

func TestExecutorBoundsProcessesAcrossReload(t *testing.T) {
	layout, err := storage.Prepare(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	oldPipeline := pipelineDefinition(t, root, "old", "old")
	newPipelines := []Pipeline{
		pipelineDefinition(t, root, "new-a", "new-a"),
		pipelineDefinition(t, root, "new-b", "new-b"),
	}
	source := newMutablePipelineSource([]Pipeline{oldPipeline})

	releaseProcesses := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseProcesses) }) }
	defer release()
	started := make(chan string, 3)
	var active atomic.Int32
	var maximum atomic.Int32
	runner := runnerFunc(func(ctx context.Context, spec processrun.Spec) processrun.Result {
		current := active.Add(1)
		for {
			old := maximum.Load()
			if current <= old || maximum.CompareAndSwap(old, current) {
				break
			}
		}
		defer active.Add(-1)
		started <- spec.Argv[0]
		select {
		case <-releaseProcesses:
		case <-ctx.Done():
		}
		return processrun.Result{ExitCode: 1, Err: errors.New("rejected")}
	})
	executor, err := NewDynamic([]Pipeline{oldPipeline}, source, runner, layout, 2, discardLogger())
	if err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan struct{})
	go func() {
		executor.Handle(context.Background(), testDocument(t, "d"))
		close(firstDone)
	}()
	select {
	case command := <-started:
		if command != "old" {
			t.Fatalf("first command = %q, want old", command)
		}
	case <-time.After(time.Second):
		t.Fatal("old pipeline did not start")
	}

	revision := source.set(newPipelines, nil)
	secondDone := make(chan struct{})
	go func() {
		executor.Handle(context.Background(), testDocument(t, "e"))
		close(secondDone)
	}()
	source.waitForLoad(t, revision)
	select {
	case command := <-started:
		if command != "new-a" && command != "new-b" {
			t.Fatalf("second command = %q, want a reloaded pipeline", command)
		}
	case <-time.After(time.Second):
		t.Fatal("a reloaded pipeline did not start in the remaining process slot")
	}
	select {
	case command := <-started:
		t.Fatalf("command %q exceeded the global process limit across reload", command)
	case <-time.After(30 * time.Millisecond):
	}

	release()
	for name, done := range map[string]<-chan struct{}{"old snapshot": firstDone, "new snapshot": secondDone} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s did not finish", name)
		}
	}
	if got := maximum.Load(); got != 2 {
		t.Fatalf("maximum parallel processes across reload = %d, want 2", got)
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

func assertCommands(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("commands = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("commands = %v, want %v", got, want)
		}
	}
}

func assertCommandCounts(t *testing.T, got []string, want map[string]int) {
	t.Helper()
	counts := make(map[string]int, len(got))
	for _, command := range got {
		counts[command]++
	}
	if len(counts) != len(want) {
		t.Fatalf("command counts = %v, want %v", counts, want)
	}
	for command, wantCount := range want {
		if counts[command] != wantCount {
			t.Fatalf("command counts = %v, want %v", counts, want)
		}
	}
}

func waitForGateRefs(t *testing.T, executor *Executor, pipelineID string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	canonicalID := strings.ToLower(pipelineID)
	for {
		executor.gatesMu.Lock()
		gate := executor.gates[canonicalID]
		refs := 0
		if gate != nil {
			refs = gate.refs
		}
		executor.gatesMu.Unlock()
		if refs >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pipeline gate refs = %d, want at least %d", refs, want)
		}
		time.Sleep(time.Millisecond)
	}
}
