package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHelpAndVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := Run(context.Background(), []string{"--help"}, &stdout, &stderr, BuildInfo{}); code != 0 {
		t.Fatalf("help exit code = %d", code)
	}
	if !strings.Contains(stdout.String(), "boarkshop validate") {
		t.Fatalf("unexpected help: %s", stdout.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := Run(context.Background(), []string{"serve", "-h"}, &stdout, &stderr, BuildInfo{}); code != 0 {
		t.Fatalf("serve help exit code = %d", code)
	}

	stdout.Reset()
	stderr.Reset()
	build := BuildInfo{Version: "v0.1.0", Commit: "abc", Date: "today"}
	if code := Run(context.Background(), []string{"version"}, &stdout, &stderr, build); code != 0 {
		t.Fatalf("version exit code = %d", code)
	}
	if got := stdout.String(); !strings.Contains(got, "v0.1.0") || !strings.Contains(got, "abc") {
		t.Fatalf("unexpected version output: %s", got)
	}
}

func TestValidateCommand(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "boarkshop.yaml")
	if err := os.WriteFile(path, []byte("version: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := Run(context.Background(), []string{"validate", "--config", path}, &stdout, &stderr, BuildInfo{}); code != 0 {
		t.Fatalf("validate exit code = %d, stderr=%s", code, stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "configuration is valid" {
		t.Fatalf("validate output = %q", got)
	}
}

func TestServeRejectsInvalidLogLevelBeforeStarting(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Run(context.Background(), []string{"serve", "--log-level", "nope"}, &stdout, &stderr, BuildInfo{})
	if code != 2 || !strings.Contains(stderr.String(), "invalid log level") {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}
