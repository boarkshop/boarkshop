package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPipelineDirectorySourceReloadsDefinitionsAndSecrets(t *testing.T) {
	root := t.TempDir()
	pipelineDir := filepath.Join(root, "dynamic")
	secretPath := filepath.Join(pipelineDir, "token")
	manifestPath := filepath.Join(pipelineDir, "pipeline.yaml")
	writeFile(t, secretPath, "first\n")
	writeFile(t, manifestPath, `version: 1
id: dynamic
secrets:
  TOKEN:
    file: token
guard:
  argv: [guard-v1]
steps:
  - id: run
    argv: [step-v1]
`)

	source := pipelineDirectorySource{root: root}
	first, err := source.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].Guard.Argv[0] != "guard-v1" || first[0].SecretEnv["TOKEN"] != "first" {
		t.Fatalf("first pipeline generation = %#v", first)
	}

	writeFile(t, secretPath, "second\n")
	writeFile(t, manifestPath, `version: 1
id: dynamic
secrets:
  TOKEN:
    file: token
guard:
  argv: [guard-v2]
steps:
  - id: run
    argv: [step-v2]
`)
	second, err := source.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].Guard.Argv[0] != "guard-v2" || second[0].SecretEnv["TOKEN"] != "second" {
		t.Fatalf("second pipeline generation = %#v", second)
	}

	writeFile(t, manifestPath, `version: 1
id: dynamic
enabled: false
guard:
  argv: [guard-disabled]
`)
	disabled, err := source.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(disabled) != 0 {
		t.Fatalf("disabled pipeline generation = %#v, want empty", disabled)
	}

	if err := os.Remove(manifestPath); err != nil {
		t.Fatal(err)
	}
	removed, err := source.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 {
		t.Fatalf("removed pipeline generation = %#v, want empty", removed)
	}
}

func TestPipelineDirectorySourceRejectsWholeInvalidCatalog(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "valid", "pipeline.yaml"), `version: 1
id: valid
guard:
  argv: [guard]
`)
	writeFile(t, filepath.Join(root, "broken", "pipeline.yaml"), `version: 1
id: broken
unknown: true
guard:
  argv: [guard]
`)

	pipelines, err := (pipelineDirectorySource{root: root}).Load()
	if err == nil {
		t.Fatal("invalid catalog was accepted")
	}
	if len(pipelines) != 0 {
		t.Fatalf("invalid catalog returned partial pipelines: %#v", pipelines)
	}
}

func TestPipelineDirectorySourceMissingRootPolicy(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing")
	if _, err := (pipelineDirectorySource{root: root}).Load(); err == nil {
		t.Fatal("strict runtime source accepted a missing root")
	}
	pipelines, err := (pipelineDirectorySource{root: root, allowMissing: true}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(pipelines) != 0 {
		t.Fatalf("validation source returned pipelines for a missing root: %#v", pipelines)
	}
}
