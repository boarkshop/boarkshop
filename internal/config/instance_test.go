package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadInstanceDefaultsAndRelativePaths(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "boarkshop.yaml")
	writeTestFile(t, path, `
version: 1
data_dir: var/data
pipelines_dir: installed
listeners:
  http:
    enabled: true
  cron:
    schedules:
      - id: every-hour
        expression: "0 * * * *"
`)

	config, err := LoadInstance(path)
	if err != nil {
		t.Fatalf("LoadInstance() error = %v", err)
	}
	if config.DataDir != filepath.Join(dir, "var", "data") {
		t.Errorf("DataDir = %q", config.DataDir)
	}
	if config.PipelinesDir != filepath.Join(dir, "installed") {
		t.Errorf("PipelinesDir = %q", config.PipelinesDir)
	}
	if config.QueueSize != DefaultQueueSize {
		t.Errorf("QueueSize = %d", config.QueueSize)
	}
	if config.MaxParallelProcesses != DefaultMaxParallelProcesses {
		t.Errorf("MaxParallelProcesses = %d", config.MaxParallelProcesses)
	}
	if config.ShutdownTimeout.Std() != 30*time.Second {
		t.Errorf("ShutdownTimeout = %s", config.ShutdownTimeout)
	}
	if !config.Listeners.HTTP.Enabled {
		t.Error("HTTP listener is disabled")
	}
	if config.Listeners.HTTP.Address != DefaultHTTPAddress {
		t.Errorf("HTTP address = %q", config.Listeners.HTTP.Address)
	}
	if config.Listeners.Telegram.BotsDir != filepath.Join(dir, "bots") {
		t.Errorf("BotsDir = %q", config.Listeners.Telegram.BotsDir)
	}
	if config.Listeners.Telegram.ReloadInterval != DefaultTelegramReloadInterval {
		t.Errorf("ReloadInterval = %s", config.Listeners.Telegram.ReloadInterval)
	}
	if config.Listeners.Cron.Timezone != "UTC" {
		t.Errorf("cron timezone = %q", config.Listeners.Cron.Timezone)
	}
}

func TestLoadInstanceRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "boarkshop.yaml")
	writeTestFile(t, path, `
version: 1
listeners:
  http:
    enabled: true
    mystery: value
`)

	_, err := LoadInstance(path)
	if err == nil || !strings.Contains(err.Error(), "field mystery not found") {
		t.Fatalf("LoadInstance() error = %v, want strict unknown-field error", err)
	}
}

func TestLoadInstanceValidation(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		message string
	}{
		{
			name:    "version",
			yaml:    `version: 2`,
			message: "version must be 1",
		},
		{
			name:    "queue size",
			yaml:    "version: 1\nqueue_size: -1\n",
			message: "queue_size must be positive",
		},
		{
			name: "nested runtime directories",
			yaml: `
version: 1
data_dir: runtime
pipelines_dir: runtime/pipelines
`,
			message: "must be separate, non-nested directories",
		},
		{
			name: "timezone",
			yaml: `
version: 1
listeners:
  cron:
    timezone: Mars/Olympus
`,
			message: "invalid timezone",
		},
		{
			name: "cron expression",
			yaml: `
version: 1
listeners:
  cron:
    schedules:
      - id: broken
        expression: definitely-not-cron
`,
			message: "expression",
		},
		{
			name: "cron descriptor",
			yaml: `
version: 1
listeners:
  cron:
    schedules:
      - id: descriptor
        expression: "@every 1m"
`,
			message: "exactly five fields",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "boarkshop.yaml")
			writeTestFile(t, path, test.yaml)
			_, err := LoadInstance(path)
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("LoadInstance() error = %v, want containing %q", err, test.message)
			}
		})
	}
}

func TestLoadInstanceRejectsBotsField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "boarkshop.yaml")
	writeTestFile(t, path, `
version: 1
listeners:
  telegram:
    bots: []
`)
	_, err := LoadInstance(path)
	if err == nil || !strings.Contains(err.Error(), "field bots not found") {
		t.Fatalf("LoadInstance() error = %v, want removed-field error", err)
	}
}

func TestLoadInstanceRejectsMultipleDocuments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "boarkshop.yaml")
	writeTestFile(t, path, "version: 1\n---\nversion: 1\n")
	_, err := LoadInstance(path)
	if err == nil || !strings.Contains(err.Error(), "multiple YAML documents") {
		t.Fatalf("LoadInstance() error = %v", err)
	}
}

func writeTestFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", path, err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimSpace(contents)+"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile(%q): %v", path, err)
	}
}
