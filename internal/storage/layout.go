package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

var safeID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

// Layout contains all mutable runtime directories. Pipeline definitions live
// separately and are never modified by this package.
type Layout struct {
	DataDir      string
	RunsDir      string
	PipelinesDir string
	SharedDir    string
}

func Prepare(dataDir string) (Layout, error) {
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return Layout{}, fmt.Errorf("resolve data directory: %w", err)
	}

	layout := Layout{
		DataDir:      abs,
		RunsDir:      filepath.Join(abs, "runs"),
		PipelinesDir: filepath.Join(abs, "pipelines"),
		SharedDir:    filepath.Join(abs, "shared"),
	}
	for _, path := range []string{layout.DataDir, layout.RunsDir, layout.PipelinesDir, layout.SharedDir} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return Layout{}, fmt.Errorf("create runtime directory %q: %w", path, err)
		}
		if err := os.Chmod(path, 0o700); err != nil {
			return Layout{}, fmt.Errorf("secure runtime directory %q: %w", path, err)
		}
	}
	return layout, nil
}

func (l Layout) PipelineData(pipelineID string) (string, error) {
	if !safeID.MatchString(pipelineID) {
		return "", fmt.Errorf("invalid pipeline id %q", pipelineID)
	}
	path := filepath.Join(l.PipelinesDir, pipelineID)
	if err := os.MkdirAll(path, 0o700); err != nil {
		return "", fmt.Errorf("create data directory for pipeline %q: %w", pipelineID, err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return "", fmt.Errorf("secure data directory for pipeline %q: %w", pipelineID, err)
	}
	return path, nil
}

// NewRun creates a private per-run directory under the managed run root.
func (l Layout) NewRun(pipelineID string) (string, error) {
	if !safeID.MatchString(pipelineID) {
		return "", fmt.Errorf("invalid pipeline id %q", pipelineID)
	}
	path, err := os.MkdirTemp(l.RunsDir, pipelineID+"-")
	if err != nil {
		return "", fmt.Errorf("create run directory: %w", err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		_ = os.RemoveAll(path)
		return "", fmt.Errorf("secure run directory: %w", err)
	}
	return path, nil
}

// CleanupRuns removes run directories left behind by an unclean shutdown.
func (l Layout) CleanupRuns() error {
	entries, err := os.ReadDir(l.RunsDir)
	if err != nil {
		return fmt.Errorf("read run directory: %w", err)
	}
	for _, entry := range entries {
		path := filepath.Join(l.RunsDir, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("remove orphaned run directory %q: %w", path, err)
		}
	}
	return nil
}
