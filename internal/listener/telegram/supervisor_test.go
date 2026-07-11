package telegram

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/boarkshop/boarkshop/internal/event"
	"github.com/boarkshop/boarkshop/internal/listener"
)

func TestSupervisorHotAddsRemovesAndReplacesBots(t *testing.T) {
	source := &mutableBotSource{}
	transport := newTrackingTransport()
	supervisor, err := NewSupervisor(SupervisorConfig{
		Source:         source,
		ReloadInterval: 5 * time.Millisecond,
		RetryDelay:     time.Millisecond,
		HTTPClient:     &http.Client{Transport: transport},
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, listener.SinkFunc(func(context.Context, event.Document) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx) }()

	main := Bot{ID: "main", Token: "token-main", APIBase: "https://telegram.test"}
	ops := Bot{ID: "ops", Token: "token-ops", APIBase: "https://telegram.test"}
	source.Set([]Bot{main})
	waitTransportEvent(t, transport.events, "start:token-main")
	source.Fail(errors.New("catalog is temporarily invalid"))
	time.Sleep(20 * time.Millisecond)
	if starts := transport.StartCount("token-main"); starts != 1 {
		t.Fatalf("last-good bot restarted after catalog error: starts=%d", starts)
	}

	source.Set([]Bot{main, ops})
	waitTransportEvent(t, transport.events, "start:token-ops")
	time.Sleep(30 * time.Millisecond)
	if starts := transport.StartCount("token-main"); starts != 1 {
		t.Fatalf("unchanged main bot started %d times", starts)
	}

	source.Set([]Bot{main})
	waitTransportEvent(t, transport.events, "stop:token-ops")

	rotated := main
	rotated.Token = "token-rotated"
	transport.ResetOverlap()
	source.Set([]Bot{rotated})
	waitTransportEvent(t, transport.events, "stop:token-main")
	waitTransportEvent(t, transport.events, "start:token-rotated")
	if transport.Overlapped() {
		t.Fatal("replacement pollers overlapped")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not stop")
	}
}

func TestSupervisorIsolatesFatalBotErrorAndRedactsToken(t *testing.T) {
	const badToken = "highly-secret-bad-token"
	source := &mutableBotSource{}
	transport := newTrackingTransport()
	transport.unauthorized[badToken] = true
	bots := []Bot{
		{ID: "good", Token: "good-token", APIBase: "https://telegram.test"},
		{ID: "bad", Token: badToken, APIBase: "https://telegram.test"},
	}
	source.Set(bots)
	var logs bytes.Buffer
	supervisor, err := NewSupervisor(SupervisorConfig{
		Initial:        bots,
		Source:         source,
		ReloadInterval: 5 * time.Millisecond,
		RetryDelay:     time.Millisecond,
		HTTPClient:     &http.Client{Transport: transport},
		Logger:         slog.New(slog.NewJSONHandler(&logs, nil)),
	}, listener.SinkFunc(func(context.Context, event.Document) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx) }()
	waitTransportEvent(t, transport.events, "start:good-token")
	waitFor(t, time.Second, func() bool { return transport.StartCount(badToken) == 1 })
	time.Sleep(40 * time.Millisecond)
	if starts := transport.StartCount(badToken); starts != 1 {
		t.Fatalf("failed bot restarted without a config change: starts=%d", starts)
	}
	select {
	case err := <-done:
		t.Fatalf("fatal bot stopped supervisor: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not stop")
	}
	if strings.Contains(logs.String(), badToken) {
		t.Fatalf("logs leaked token: %s", logs.String())
	}
}

type mutableBotSource struct {
	mu   sync.Mutex
	bots []Bot
	err  error
}

func (source *mutableBotSource) Load() ([]Bot, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	return append([]Bot(nil), source.bots...), source.err
}

func (source *mutableBotSource) Set(bots []Bot) {
	source.mu.Lock()
	source.bots = append([]Bot(nil), bots...)
	source.err = nil
	source.mu.Unlock()
}

func (source *mutableBotSource) Fail(err error) {
	source.mu.Lock()
	source.err = err
	source.mu.Unlock()
}

type trackingTransport struct {
	mu           sync.Mutex
	starts       map[string]int
	active       map[string]int
	unauthorized map[string]bool
	overlapped   bool
	events       chan string
}

func newTrackingTransport() *trackingTransport {
	return &trackingTransport{
		starts:       make(map[string]int),
		active:       make(map[string]int),
		unauthorized: make(map[string]bool),
		events:       make(chan string, 32),
	}
}

func (transport *trackingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	token := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, "/bot"), "/getUpdates")
	transport.mu.Lock()
	transport.starts[token]++
	for _, count := range transport.active {
		if count > 0 {
			transport.overlapped = true
			break
		}
	}
	transport.active[token]++
	unauthorized := transport.unauthorized[token]
	transport.mu.Unlock()
	transport.events <- "start:" + token

	if unauthorized {
		transport.mu.Lock()
		transport.active[token]--
		transport.mu.Unlock()
		return &http.Response{
			StatusCode: http.StatusUnauthorized,
			Status:     "401 Unauthorized",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":false,"error_code":401,"description":"invalid ` + token + `"}`)),
			Request:    request,
		}, nil
	}

	<-request.Context().Done()
	transport.mu.Lock()
	transport.active[token]--
	transport.mu.Unlock()
	transport.events <- "stop:" + token
	return nil, request.Context().Err()
}

func (transport *trackingTransport) StartCount(token string) int {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.starts[token]
}

func (transport *trackingTransport) Overlapped() bool {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.overlapped
}

func (transport *trackingTransport) ResetOverlap() {
	transport.mu.Lock()
	transport.overlapped = false
	transport.mu.Unlock()
}

func waitTransportEvent(t *testing.T, events <-chan string, want string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case event := <-events:
			if event == want {
				return
			}
		case <-deadline:
			t.Fatalf("did not observe transport event %q", want)
		}
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not satisfied before timeout")
		}
		time.Sleep(time.Millisecond)
	}
}
