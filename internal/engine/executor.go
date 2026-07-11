package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/boarkshop/boarkshop/internal/event"
	processrun "github.com/boarkshop/boarkshop/internal/process"
	"github.com/boarkshop/boarkshop/internal/storage"
)

const eventFilename = "event.json"

type Step struct {
	ID      string
	Argv    []string
	Timeout time.Duration
}

// Pipeline is an immutable runtime snapshot. SecretEnv contains already
// resolved values and must never be logged.
type Pipeline struct {
	ID        string
	Directory string
	Env       map[string]string
	SecretEnv map[string]string
	Guard     Step
	Steps     []Step
}

type Runner interface {
	Run(context.Context, processrun.Spec) processrun.Result
}

type runtimePipeline struct {
	definition Pipeline
	turn       chan struct{}
}

// Executor fans each event out to every active pipeline. Runs of one pipeline
// are serialized, while a shared semaphore bounds child-process concurrency.
type Executor struct {
	pipelines []*runtimePipeline
	runner    Runner
	layout    storage.Layout
	processes chan struct{}
	logger    *slog.Logger
}

func New(pipelines []Pipeline, runner Runner, layout storage.Layout, maxProcesses int, logger *slog.Logger) (*Executor, error) {
	if runner == nil {
		return nil, fmt.Errorf("runner is required")
	}
	if maxProcesses <= 0 {
		return nil, fmt.Errorf("max process count must be greater than zero")
	}
	if logger == nil {
		logger = slog.Default()
	}

	seen := make(map[string]struct{}, len(pipelines))
	runtimePipelines := make([]*runtimePipeline, 0, len(pipelines))
	for _, definition := range pipelines {
		canonicalID := strings.ToLower(definition.ID)
		if _, ok := seen[canonicalID]; ok {
			return nil, fmt.Errorf("duplicate pipeline id %q", definition.ID)
		}
		seen[canonicalID] = struct{}{}
		abs, err := filepath.Abs(definition.Directory)
		if err != nil {
			return nil, fmt.Errorf("resolve directory for pipeline %q: %w", definition.ID, err)
		}
		definition.Directory = abs
		turn := make(chan struct{}, 1)
		turn <- struct{}{}
		runtimePipelines = append(runtimePipelines, &runtimePipeline{definition: definition, turn: turn})
	}

	return &Executor{
		pipelines: runtimePipelines,
		runner:    runner,
		layout:    layout,
		processes: make(chan struct{}, maxProcesses),
		logger:    logger,
	}, nil
}

func (e *Executor) Handle(ctx context.Context, document event.Document) {
	var wg sync.WaitGroup
	for _, pipeline := range e.pipelines {
		pipeline := pipeline
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.runPipeline(ctx, pipeline, document)
		}()
	}
	wg.Wait()
}

func (e *Executor) runPipeline(ctx context.Context, pipeline *runtimePipeline, document event.Document) {
	select {
	case <-ctx.Done():
		return
	case <-pipeline.turn:
	}
	defer func() { pipeline.turn <- struct{}{} }()

	runID, err := newID()
	if err != nil {
		e.logger.Error("run id generation failed", "event_id", document.EventID, "pipeline_id", pipeline.definition.ID, "error", err)
		return
	}
	runDir, err := e.layout.NewRun(pipeline.definition.ID)
	if err != nil {
		e.logger.Error("run directory creation failed", "event_id", document.EventID, "run_id", runID, "pipeline_id", pipeline.definition.ID, "error", err)
		return
	}
	defer func() {
		if err := os.RemoveAll(runDir); err != nil {
			e.logger.Warn("run directory cleanup failed", "event_id", document.EventID, "run_id", runID, "pipeline_id", pipeline.definition.ID, "error", err)
		}
	}()

	stateDir, err := e.layout.PipelineData(pipeline.definition.ID)
	if err != nil {
		e.logRunError(document.EventID, runID, pipeline.definition.ID, "prepare_state", err)
		return
	}
	eventPath := filepath.Join(runDir, eventFilename)
	if err := os.WriteFile(eventPath, document.Root(), 0o600); err != nil {
		e.logRunError(document.EventID, runID, pipeline.definition.ID, "write_event", err)
		return
	}

	startedAt := time.Now()
	baseLog := e.logger.With("event_id", document.EventID, "run_id", runID, "pipeline_id", pipeline.definition.ID)
	baseLog.Info("pipeline run started")

	guardResult := e.runStep(ctx, pipeline.definition, pipeline.definition.Guard, document, runID, runDir, stateDir)
	switch {
	case guardResult.TimedOut:
		baseLog.Error("pipeline run failed", "status", "guard_timeout", "duration_ms", elapsedMilliseconds(startedAt))
		return
	case guardResult.Canceled:
		baseLog.Warn("pipeline run canceled", "status", "canceled", "duration_ms", elapsedMilliseconds(startedAt))
		return
	case guardResult.ExitCode == 1:
		baseLog.Info("pipeline run rejected", "status", "rejected", "duration_ms", elapsedMilliseconds(startedAt))
		return
	case guardResult.ExitCode != 0 || guardResult.Err != nil:
		baseLog.Error("pipeline run failed", "status", "guard_error", "duration_ms", elapsedMilliseconds(startedAt))
		return
	}

	for _, step := range pipeline.definition.Steps {
		result := e.runStep(ctx, pipeline.definition, step, document, runID, runDir, stateDir)
		if result.ExitCode != 0 || result.Err != nil {
			status := "step_error"
			if result.TimedOut {
				status = "step_timeout"
			} else if result.Canceled {
				status = "canceled"
			}
			baseLog.Error("pipeline run failed", "status", status, "failed_step_id", step.ID, "duration_ms", elapsedMilliseconds(startedAt))
			return
		}
	}

	baseLog.Info("pipeline run completed", "status", "succeeded", "duration_ms", elapsedMilliseconds(startedAt))
}

func (e *Executor) runStep(
	ctx context.Context,
	pipeline Pipeline,
	step Step,
	document event.Document,
	runID string,
	runDir string,
	stateDir string,
) processrun.Result {
	stepLog := e.logger.With(
		"event_id", document.EventID,
		"run_id", runID,
		"pipeline_id", pipeline.ID,
		"step_id", step.ID,
	)

	select {
	case <-ctx.Done():
		return processrun.Result{ExitCode: -1, Canceled: true, Err: ctx.Err()}
	case e.processes <- struct{}{}:
	}
	defer func() { <-e.processes }()

	result := e.runner.Run(ctx, processrun.Spec{
		Argv:    expandArgv(step.Argv, pipeline.Directory, runDir, stateDir, e.layout.SharedDir),
		Cwd:     runDir,
		Env:     commandEnvironment(pipeline, step.ID, document, runID, runDir, stateDir, e.layout.SharedDir),
		Timeout: step.Timeout,
	})

	attrs := []any{
		"duration_ms", float64(result.Duration.Microseconds()) / 1000,
		"exit_code", result.ExitCode,
		"timed_out", result.TimedOut,
		"canceled", result.Canceled,
	}
	if step.ID == "guard" && result.ExitCode == 1 && !result.TimedOut && !result.Canceled {
		stepLog.Info("pipeline step finished", attrs...)
	} else if result.Canceled {
		stepLog.Warn("pipeline step finished", attrs...)
	} else if result.Err != nil {
		attrs = append(attrs, "error", result.Err)
		stepLog.Error("pipeline step finished", attrs...)
	} else {
		stepLog.Info("pipeline step finished", attrs...)
	}
	if len(result.Stdout) > 0 || len(result.Stderr) > 0 {
		stepLog.Debug("pipeline step output",
			"stdout", string(result.Stdout),
			"stderr", string(result.Stderr),
			"stdout_truncated", result.StdoutTruncated,
			"stderr_truncated", result.StderrTruncated,
		)
	}
	return result
}

func expandArgv(argv []string, pipelineDir, runDir, stateDir, sharedDir string) []string {
	replacements := []struct {
		placeholder string
		value       string
	}{
		{"{{pipeline_dir}}", pipelineDir},
		{"{{run_dir}}", runDir},
		{"{{data_dir}}", stateDir},
		{"{{shared_dir}}", sharedDir},
		{"{{event_file}}", filepath.Join(runDir, eventFilename)},
	}
	expanded := make([]string, len(argv))
	for index, argument := range argv {
		for _, replacement := range replacements {
			argument = strings.ReplaceAll(argument, replacement.placeholder, replacement.value)
		}
		expanded[index] = argument
	}
	return expanded
}

func commandEnvironment(pipeline Pipeline, stepID string, document event.Document, runID, runDir, stateDir, sharedDir string) []string {
	env := make(map[string]string)
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			env[key] = value
		}
	}
	for key, value := range pipeline.Env {
		env[key] = value
	}
	for key, value := range pipeline.SecretEnv {
		env[key] = value
	}

	reserved := map[string]string{
		"BOARKSHOP_PIPELINE_DIR": pipeline.Directory,
		"BOARKSHOP_RUN_DIR":      runDir,
		"BOARKSHOP_DATA_DIR":     stateDir,
		"BOARKSHOP_SHARED_DIR":   sharedDir,
		"BOARKSHOP_EVENT_FILE":   filepath.Join(runDir, eventFilename),
		"BOARKSHOP_EVENT_ID":     document.EventID,
		"BOARKSHOP_RUN_ID":       runID,
		"BOARKSHOP_PIPELINE_ID":  pipeline.ID,
		"BOARKSHOP_STEP_ID":      stepID,
		"BOARKSHOP_SOURCE":       document.Source,
	}
	for key, value := range reserved {
		env[key] = value
	}

	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+env[key])
	}
	return result
}

func (e *Executor) logRunError(eventID, runID, pipelineID, stage string, err error) {
	e.logger.Error("pipeline run failed", "event_id", eventID, "run_id", runID, "pipeline_id", pipelineID, "stage", stage, "error", err)
}

func newID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(random[:]), nil
}

func elapsedMilliseconds(startedAt time.Time) float64 {
	return float64(time.Since(startedAt).Microseconds()) / 1000
}
