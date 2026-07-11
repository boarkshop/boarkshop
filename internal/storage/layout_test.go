package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLayoutKeepsPipelineDataAndCleansRuns(t *testing.T) {
	layout, err := Prepare(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}

	state, err := layout.PipelineData("pipeline-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "state"), []byte("kept"), 0o600); err != nil {
		t.Fatal(err)
	}
	run, err := layout.NewRun("pipeline-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(run, "temporary"), []byte("removed"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := layout.CleanupRuns(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(run); !os.IsNotExist(err) {
		t.Fatalf("run directory still exists: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(state, "state")); err != nil || string(got) != "kept" {
		t.Fatalf("pipeline state was not preserved: %q, %v", got, err)
	}
}

func TestLayoutRejectsUnsafePipelineID(t *testing.T) {
	layout, err := Prepare(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"../escape", "nested/path", "", ".hidden"} {
		if _, err := layout.PipelineData(id); err == nil {
			t.Errorf("PipelineData(%q) unexpectedly succeeded", id)
		}
	}
}
