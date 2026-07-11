package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPToPipelineEndToEnd(t *testing.T) {
	root := t.TempDir()
	address := availableAddress(t)
	pipelineDir := filepath.Join(root, "pipelines", "http-demo")
	if err := os.MkdirAll(pipelineDir, 0o700); err != nil {
		t.Fatal(err)
	}

	manifest := fmt.Sprintf(`
version: 1
id: http-demo
env:
  GO_WANT_BOARKSHOP_HELPER: "1"
guard:
  argv: [%q, "-test.run=TestBoarkshopHelperProcess", "--", "guard"]
  timeout: 2s
steps:
  - id: write
    argv: [%q, "-test.run=TestBoarkshopHelperProcess", "--", "write"]
    timeout: 2s
  - id: finish
    argv: [%q, "-test.run=TestBoarkshopHelperProcess", "--", "finish"]
    timeout: 2s
`, os.Args[0], os.Args[0], os.Args[0])
	writeFile(t, filepath.Join(pipelineDir, "pipeline.yaml"), manifest)

	configPath := filepath.Join(root, "boarkshop.yaml")
	writeFile(t, configPath, fmt.Sprintf(`
version: 1
data_dir: data
pipelines_dir: pipelines
queue_size: 8
max_parallel_processes: 2
shutdown_timeout: 3s
listeners:
  http:
    enabled: true
    address: %q
    max_body_bytes: 4096
    read_header_timeout: 2s
  cron:
    timezone: UTC
`, address))

	if err := Validate(configPath); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, configPath, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	}()

	response := postEventually(t, "http://"+address+"/webhooks/test", []byte(`{"hello":"world"}`))
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("HTTP status = %d, want 202", response.StatusCode)
	}
	_ = response.Body.Close()

	stateMarker := filepath.Join(root, "data", "pipelines", "http-demo", "complete")
	waitForFile(t, stateMarker, 3*time.Second)
	if got, err := os.ReadFile(stateMarker); err != nil || string(got) != "next" {
		t.Fatalf("state marker = %q, %v", got, err)
	}
	sharedMarker := filepath.Join(root, "data", "shared", "seen")
	if got, err := os.ReadFile(sharedMarker); err != nil || string(got) != "http" {
		t.Fatalf("shared marker = %q, %v", got, err)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not stop")
	}

	runs, err := os.ReadDir(filepath.Join(root, "data", "runs"))
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 0 {
		t.Fatalf("temporary run directories remain: %v", runs)
	}
	if got, err := os.ReadFile(stateMarker); err != nil || string(got) != "next" {
		t.Fatalf("pipeline state was not persistent after shutdown: %q, %v", got, err)
	}
}

func TestRunLoadsPipelineAddedAfterStartup(t *testing.T) {
	root := t.TempDir()
	address := availableAddress(t)
	pipelinesDir := filepath.Join(root, "pipelines")
	if err := os.MkdirAll(pipelinesDir, 0o700); err != nil {
		t.Fatal(err)
	}

	configPath := filepath.Join(root, "boarkshop.yaml")
	writeFile(t, configPath, fmt.Sprintf(`
version: 1
data_dir: data
pipelines_dir: pipelines
queue_size: 8
max_parallel_processes: 2
shutdown_timeout: 3s
listeners:
  http:
    enabled: true
    address: %q
    max_body_bytes: 4096
    read_header_timeout: 2s
  cron:
    timezone: UTC
`, address))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, configPath, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon did not stop")
		}
	}()

	// Observe that the listener is ready without submitting an event. The first
	// event must therefore be handled only after the pipeline is published.
	waitForTCP(t, address, 3*time.Second)

	stagingDir := filepath.Join(root, "hot-add-staging")
	manifest := fmt.Sprintf(`
version: 1
id: hot-added
env:
  GO_WANT_BOARKSHOP_HELPER: "1"
guard:
  argv: [%q, "-test.run=TestBoarkshopHelperProcess", "--", "guard"]
  timeout: 2s
steps:
  - id: write
    argv: [%q, "-test.run=TestBoarkshopHelperProcess", "--", "write"]
    timeout: 2s
  - id: finish
    argv: [%q, "-test.run=TestBoarkshopHelperProcess", "--", "finish"]
    timeout: 2s
`, os.Args[0], os.Args[0], os.Args[0])
	writeFile(t, filepath.Join(stagingDir, "pipeline.yaml"), manifest)
	if err := os.Rename(stagingDir, filepath.Join(pipelinesDir, "hot-added")); err != nil {
		t.Fatalf("publish pipeline: %v", err)
	}

	response := postEventually(t, "http://"+address+"/webhooks/hot-add", []byte(`{"hot":"added"}`))
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("HTTP status = %d, want 202", response.StatusCode)
	}
	_ = response.Body.Close()

	stateMarker := filepath.Join(root, "data", "pipelines", "hot-added", "complete")
	waitForFile(t, stateMarker, 3*time.Second)
	if got, err := os.ReadFile(stateMarker); err != nil || string(got) != "next" {
		t.Fatalf("hot-added pipeline marker = %q, %v", got, err)
	}
}

func TestRunLoadsTelegramBotAddedAfterStartup(t *testing.T) {
	root := t.TempDir()
	var requests atomic.Int32
	telegramAPI := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if requests.Add(1) > 1 {
			select {
			case <-request.Context().Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
			response.Header().Set("Content-Type", "application/json")
			_, _ = response.Write([]byte(`{"ok":true,"result":[]}`))
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"ok":true,"result":[{"update_id":1,"message":{"chat":{"id":42},"text":"hot add"}}]}`))
	}))
	defer telegramAPI.Close()

	pipelineDir := filepath.Join(root, "pipelines", "telegram-demo")
	manifest := fmt.Sprintf(`
version: 1
id: telegram-demo
env:
  GO_WANT_BOARKSHOP_HELPER: "1"
guard:
  argv: [%q, "-test.run=TestBoarkshopHelperProcess", "--", "telegram-guard"]
  timeout: 2s
steps:
  - id: write
    argv: [%q, "-test.run=TestBoarkshopHelperProcess", "--", "write"]
    timeout: 2s
  - id: finish
    argv: [%q, "-test.run=TestBoarkshopHelperProcess", "--", "finish"]
    timeout: 2s
`, os.Args[0], os.Args[0], os.Args[0])
	writeFile(t, filepath.Join(pipelineDir, "pipeline.yaml"), manifest)

	configPath := filepath.Join(root, "boarkshop.yaml")
	writeFile(t, configPath, `
version: 1
data_dir: data
pipelines_dir: pipelines
queue_size: 8
max_parallel_processes: 2
shutdown_timeout: 3s
listeners:
  telegram:
    bots_dir: bots
    reload_interval: 10ms
  cron:
    timezone: UTC
`)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, configPath, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon did not stop")
		}
	}()

	botsDir := filepath.Join(root, "bots")
	waitForFile(t, botsDir, 3*time.Second)
	stagingDir := filepath.Join(root, "bot-staging")
	writeFile(t, filepath.Join(stagingDir, "token"), "hot-token\n")
	writeFile(t, filepath.Join(stagingDir, "bot.yaml"), fmt.Sprintf(`
version: 1
id: hot-bot
token: {file: token}
api_base: %q
poll_timeout: 1s
`, telegramAPI.URL))
	if err := os.Rename(stagingDir, filepath.Join(botsDir, "hot-bot")); err != nil {
		t.Fatalf("publish bot: %v", err)
	}

	stateMarker := filepath.Join(root, "data", "pipelines", "telegram-demo", "complete")
	waitForFile(t, stateMarker, 4*time.Second)
	if got, err := os.ReadFile(filepath.Join(root, "data", "shared", "seen")); err != nil || string(got) != "telegram" {
		t.Fatalf("shared marker = %q, %v", got, err)
	}
}

func TestValidateAllowsEmptyRuntime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "boarkshop.yaml")
	writeFile(t, path, "version: 1\n")
	if err := Validate(path); err != nil {
		t.Fatalf("empty runtime should be valid: %v", err)
	}
}

func TestBoarkshopHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_BOARKSHOP_HELPER") != "1" {
		return
	}
	action := ""
	for index, argument := range os.Args {
		if argument == "--" && index+1 < len(os.Args) {
			action = os.Args[index+1]
			break
		}
	}
	switch action {
	case "guard":
		raw, err := os.ReadFile(os.Getenv("BOARKSHOP_EVENT_FILE"))
		if err != nil {
			os.Exit(2)
		}
		var eventRoot map[string]any
		if json.Unmarshal(raw, &eventRoot) != nil || eventRoot["source"] != "http" || eventRoot["method"] != "POST" {
			os.Exit(1)
		}
		os.Exit(0)
	case "telegram-guard":
		raw, err := os.ReadFile(os.Getenv("BOARKSHOP_EVENT_FILE"))
		if err != nil {
			os.Exit(2)
		}
		var eventRoot map[string]any
		if json.Unmarshal(raw, &eventRoot) != nil || eventRoot["source"] != "telegram" || eventRoot["bot_id"] != "hot-bot" {
			os.Exit(1)
		}
		os.Exit(0)
	case "write":
		if err := os.WriteFile(filepath.Join(os.Getenv("BOARKSHOP_RUN_DIR"), "handoff"), []byte("next"), 0o600); err != nil {
			os.Exit(2)
		}
		os.Exit(0)
	case "finish":
		value, err := os.ReadFile(filepath.Join(os.Getenv("BOARKSHOP_RUN_DIR"), "handoff"))
		if err != nil {
			os.Exit(2)
		}
		if err := os.WriteFile(filepath.Join(os.Getenv("BOARKSHOP_DATA_DIR"), "complete"), value, 0o600); err != nil {
			os.Exit(2)
		}
		if err := os.WriteFile(filepath.Join(os.Getenv("BOARKSHOP_SHARED_DIR"), "seen"), []byte(os.Getenv("BOARKSHOP_SOURCE")), 0o600); err != nil {
			os.Exit(2)
		}
		os.Exit(0)
	default:
		os.Exit(2)
	}
}

func availableAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitForTCP(t *testing.T, address string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		connection, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			_ = connection.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("listener %q did not start: %v", address, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func postEventually(t *testing.T, url string, body []byte) *http.Response {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, err := client.Post(url, "application/json", bytes.NewReader(body))
		if err == nil {
			return response
		}
		if time.Now().After(deadline) {
			t.Fatalf("HTTP listener did not start: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("file %q was not created", path)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	content = strings.TrimPrefix(content, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
