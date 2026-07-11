package config

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadPipelinesFromImmediateDirectories(t *testing.T) {
	root := t.TempDir()
	pipelineDir := filepath.Join(root, "alpha")
	writeTestFile(t, filepath.Join(pipelineDir, "scripts", "guard.sh"), "#!/bin/sh")
	writeTestFile(t, filepath.Join(pipelineDir, "scripts", "process.py"), "print('ok')")
	writeTestFile(t, filepath.Join(pipelineDir, "secrets", "token"), "secret")
	writeTestFile(t, filepath.Join(pipelineDir, PipelineFilename), `
version: 1
id: alpha
env:
  MODE: test
secrets:
  TOKEN:
    file: secrets/token
  API_TOKEN:
    env: SOURCE_API_TOKEN
resources:
  - scripts/guard.sh
  - scripts/process.py
guard:
  argv: [sh, scripts/guard.sh]
steps:
  - id: process
    argv: [python, scripts/process.py]
    timeout: 2m
`)

	// A nested pipeline and an immediate directory without pipeline.yaml are
	// intentionally outside the loader's one-level discovery contract.
	writeTestFile(t, filepath.Join(root, "container", "nested", PipelineFilename), `
version: 1
id: nested
guard: {argv: ["true"]}
`)

	pipelines, err := LoadPipelines(root)
	if err != nil {
		t.Fatalf("LoadPipelines() error = %v", err)
	}
	if len(pipelines) != 1 {
		t.Fatalf("len(pipelines) = %d, want 1", len(pipelines))
	}
	pipeline := pipelines[0]
	if pipeline.ID != "alpha" || !pipeline.Enabled {
		t.Errorf("pipeline identity = %q, enabled=%v", pipeline.ID, pipeline.Enabled)
	}
	if pipeline.Guard.Timeout != DefaultCommandTimeout {
		t.Errorf("guard timeout = %s", pipeline.Guard.Timeout)
	}
	if pipeline.Steps[0].Timeout.Std() != 2*time.Minute {
		t.Errorf("step timeout = %s", pipeline.Steps[0].Timeout)
	}
	if !filepath.IsAbs(pipeline.Resources[0]) {
		t.Errorf("resource was not resolved: %q", pipeline.Resources[0])
	}
	if want := filepath.Join(pipelineDir, "secrets", "token"); pipeline.Secrets["TOKEN"].File != want {
		t.Errorf("secret file = %q, want %q", pipeline.Secrets["TOKEN"].File, want)
	}
}

func TestLoadPipelineRejectsUnknownRetryAndDedupFields(t *testing.T) {
	tests := []string{
		`retry: {max_attempts: 2}`,
		`dedup: {key: event_id}`,
	}
	for _, unknown := range tests {
		t.Run(strings.Fields(unknown)[0], func(t *testing.T) {
			dir := t.TempDir()
			writeTestFile(t, filepath.Join(dir, PipelineFilename), `
version: 1
id: strict
guard:
  argv: ["true"]
`+unknown)
			_, err := LoadPipeline(dir)
			if err == nil || !strings.Contains(err.Error(), "field") {
				t.Fatalf("LoadPipeline() error = %v, want strict field error", err)
			}
		})
	}
}

func TestLoadPipelineRejectsUnsafeReferences(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "pipeline")
	outside := filepath.Join(root, "outside-secret")
	writeTestFile(t, outside, "secret")

	tests := []struct {
		name      string
		reference string
	}{
		{name: "parent traversal", reference: "../outside-secret"},
		{name: "backslash traversal", reference: `..\outside-secret`},
		{name: "absolute", reference: outside},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writeTestFile(t, filepath.Join(dir, PipelineFilename), `
version: 1
id: unsafe
resources:
  - `+test.reference+`
guard:
  argv: ["true"]
`)
			_, err := LoadPipeline(dir)
			if err == nil || (!strings.Contains(err.Error(), "must be relative") && !strings.Contains(err.Error(), "escapes")) {
				t.Fatalf("LoadPipeline() error = %v, want unsafe-reference error", err)
			}
		})
	}
}

func TestLoadPipelineRejectsDuplicateStepIDs(t *testing.T) {
	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, PipelineFilename), `
version: 1
id: duplicate-steps
guard: {argv: ["true"]}
steps:
  - id: run
    argv: [one]
  - id: run
    argv: [two]
`)
	_, err := LoadPipeline(dir)
	if err == nil || !strings.Contains(err.Error(), `duplicate step id "run"`) {
		t.Fatalf("LoadPipeline() error = %v", err)
	}
}

func TestLoadPipelineRejectsOverlongID(t *testing.T) {
	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, PipelineFilename), `
version: 1
id: `+strings.Repeat("a", 129)+`
guard: {argv: ["true"]}
`)
	_, err := LoadPipeline(dir)
	if err == nil || !strings.Contains(err.Error(), "id") {
		t.Fatalf("LoadPipeline() error = %v, want invalid id error", err)
	}
}

func TestLoadPipelinesRejectsDuplicatePipelineIDs(t *testing.T) {
	root := t.TempDir()
	for index, dir := range []string{"one", "two"} {
		id := "same"
		if index == 0 {
			id = "Same"
		}
		writeTestFile(t, filepath.Join(root, dir, PipelineFilename), `
version: 1
id: `+id+`
guard: {argv: ["true"]}
`)
	}
	_, err := LoadPipelines(root)
	if err == nil || !strings.Contains(err.Error(), `duplicate pipeline id "same"`) {
		t.Fatalf("LoadPipelines() error = %v", err)
	}
}

func TestLoadPipelineRequiresExistingReferences(t *testing.T) {
	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, PipelineFilename), `
version: 1
id: missing-resource
resources: [missing.txt]
guard: {argv: ["true"]}
`)
	_, err := LoadPipeline(dir)
	if err == nil || !strings.Contains(err.Error(), "missing.txt") {
		t.Fatalf("LoadPipeline() error = %v", err)
	}
}

func TestLoadPipelineRejectsReservedEnvironmentNames(t *testing.T) {
	tests := []string{
		"env:\n  BOARKSHOP_RUN_DIR: override",
		"secrets:\n  boarkshop_shared_dir: {env: SOURCE_VALUE}",
	}
	for _, declaration := range tests {
		t.Run(strings.SplitN(declaration, ":", 2)[0], func(t *testing.T) {
			dir := t.TempDir()
			writeTestFile(t, filepath.Join(dir, PipelineFilename), `
version: 1
id: reserved-env
guard: {argv: ["true"]}
`+declaration)
			_, err := LoadPipeline(dir)
			if err == nil || !strings.Contains(err.Error(), "reserved BOARKSHOP_ prefix") {
				t.Fatalf("LoadPipeline() error = %v", err)
			}
		})
	}
}
