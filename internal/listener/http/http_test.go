package http

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/boarkshop/boarkshop/internal/event"
	"github.com/boarkshop/boarkshop/internal/listener"
)

type recordingSink struct {
	mu        sync.Mutex
	documents []event.Document
	err       error
	accepted  chan event.Document
}

func (sink *recordingSink) Submit(_ context.Context, document event.Document) error {
	if sink.err != nil {
		return sink.err
	}
	sink.mu.Lock()
	sink.documents = append(sink.documents, document)
	sink.mu.Unlock()
	if sink.accepted != nil {
		select {
		case sink.accepted <- document:
		default:
		}
	}
	return nil
}

func (sink *recordingSink) count() int {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return len(sink.documents)
}

func TestListenerBindsNormalizesAndShutsDown(t *testing.T) {
	sink := &recordingSink{accepted: make(chan event.Document, 1)}
	httpListener, err := New(Config{
		Address:      "127.0.0.1:0",
		MaxBodyBytes: 1024,
	}, sink)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	serveError := make(chan error, 1)
	go func() { serveError <- httpListener.Start(ctx) }()
	address := waitForAddress(t, httpListener)

	body := `{"action":"opened","number":42}`
	request, err := stdhttp.NewRequest(stdhttp.MethodPatch, "http://"+address+"/webhooks/example?tag=a&tag=b", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Delivery", "delivery-1")
	response, err := stdhttp.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != stdhttp.StatusAccepted {
		t.Fatalf("status = %d, want %d", response.StatusCode, stdhttp.StatusAccepted)
	}

	var document event.Document
	select {
	case document = <-sink.accepted:
	case <-time.After(time.Second):
		t.Fatal("listener did not submit an event")
	}
	if document.Source != "http" || document.SchemaVersion != listener.SchemaVersion {
		t.Fatalf("metadata = source %q schema %d", document.Source, document.SchemaVersion)
	}
	if document.EventID == "" || document.ReceivedAt.IsZero() {
		t.Fatalf("missing generated metadata: %#v", document)
	}

	var root struct {
		Source        string `json:"source"`
		SchemaVersion int    `json:"schema_version"`
		EventID       string `json:"event_id"`
		ReceivedAt    string `json:"received_at"`
		Method        string `json:"method"`
		Path          string `json:"path"`
		Request       struct {
			Query         map[string][]string `json:"query"`
			Headers       map[string][]string `json:"headers"`
			BodyBase64    string              `json:"body_base64"`
			BodyText      *string             `json:"body_text"`
			BodyJSON      map[string]any      `json:"body_json"`
			RemoteAddress string              `json:"remote_address"`
		} `json:"request"`
	}
	if err := json.Unmarshal(document.Root(), &root); err != nil {
		t.Fatal(err)
	}
	if root.Source != "http" || root.SchemaVersion != 1 || root.EventID != document.EventID || root.ReceivedAt == "" {
		t.Fatalf("unexpected root metadata: %#v", root)
	}
	if root.Method != stdhttp.MethodPatch || root.Path != "/webhooks/example" {
		t.Fatalf("unexpected route fields: method=%q path=%q", root.Method, root.Path)
	}
	if got := root.Request.Query["tag"]; len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("query tag = %#v", got)
	}
	if got := root.Request.Headers["X-Delivery"]; len(got) != 1 || got[0] != "delivery-1" {
		t.Fatalf("X-Delivery = %#v", got)
	}
	if got := root.Request.Headers["Host"]; len(got) != 1 || got[0] != request.Host {
		t.Fatalf("Host = %#v", got)
	}
	if root.Request.BodyBase64 != base64.StdEncoding.EncodeToString([]byte(body)) {
		t.Fatalf("body_base64 = %q", root.Request.BodyBase64)
	}
	if root.Request.BodyText == nil || *root.Request.BodyText != body {
		t.Fatalf("body_text = %#v", root.Request.BodyText)
	}
	if root.Request.BodyJSON["action"] != "opened" || root.Request.BodyJSON["number"] != float64(42) {
		t.Fatalf("body_json = %#v", root.Request.BodyJSON)
	}
	if root.Request.RemoteAddress == "" {
		t.Fatal("remote_address is empty")
	}

	cancel()
	select {
	case err := <-serveError:
		if err != nil {
			t.Fatalf("server shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not shut down after context cancellation")
	}
}

func TestListenerRejectsOversizedBody(t *testing.T) {
	sink := &recordingSink{}
	httpListener, err := New(Config{MaxBodyBytes: 3}, sink)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(stdhttp.MethodPost, "http://example.test/any", strings.NewReader("four"))
	response := httptest.NewRecorder()
	httpListener.Handler().ServeHTTP(response, request)
	if response.Code != stdhttp.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", response.Code, stdhttp.StatusRequestEntityTooLarge)
	}
	if sink.count() != 0 {
		t.Fatal("oversized request reached the sink")
	}
}

func TestListenerReturnsUnavailableOnBackpressure(t *testing.T) {
	sink := &recordingSink{err: errors.New("queue full")}
	httpListener, err := New(Config{MaxBodyBytes: 16}, sink)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(stdhttp.MethodDelete, "http://example.test/every/path", nil)
	response := httptest.NewRecorder()
	httpListener.Handler().ServeHTTP(response, request)
	if response.Code != stdhttp.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, stdhttp.StatusServiceUnavailable)
	}
}

func waitForAddress(t *testing.T, httpListener *Listener) string {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if address := httpListener.Addr(); address != nil {
			return address.String()
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("HTTP listener did not bind")
	return ""
}
