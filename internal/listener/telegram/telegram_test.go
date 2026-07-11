package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/boarkshop/boarkshop/internal/event"
	"github.com/boarkshop/boarkshop/internal/listener"
)

func TestListenerPollsMultipleBotsAndNormalizesUpdates(t *testing.T) {
	updates := map[string]string{
		"token-main": `{"update_id":100,"message":{"message_id":1,"from":{"id":7},"chat":{"id":11},"text":"hello"}}`,
		"token-ops":  `{"update_id":200,"callback_query":{"id":"callback","from":{"id":8},"message":{"chat":{"id":12}}}}`,
	}
	var mu sync.Mutex
	requests := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		token := tokenFromPath(t, request.URL.Path)
		if err := request.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		if request.Form.Get("timeout") == "" || request.Form.Get("offset") == "" {
			t.Errorf("missing long-poll fields: %#v", request.Form)
		}
		mu.Lock()
		requests[token]++
		requestNumber := requests[token]
		mu.Unlock()
		if requestNumber > 1 {
			<-request.Context().Done()
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(response, `{"ok":true,"result":[%s]}`, updates[token])
	}))
	defer server.Close()

	documents := make(chan event.Document, 2)
	telegramListener, err := New(Config{
		Bots: []Bot{
			{ID: "main", Token: "token-main", APIBase: server.URL, PollTimeout: time.Second},
			{ID: "ops", Token: "token-ops", APIBase: server.URL, PollTimeout: time.Second},
		},
		RetryDelay: 5 * time.Millisecond,
	}, listener.SinkFunc(func(_ context.Context, document event.Document) error {
		documents <- document
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runError := make(chan error, 1)
	go func() { runError <- telegramListener.Run(ctx) }()

	seen := make(map[string]telegramRoot, 2)
	for len(seen) < 2 {
		select {
		case document := <-documents:
			if document.Source != "telegram" || document.SchemaVersion != listener.SchemaVersion {
				t.Fatalf("unexpected metadata: %#v", document)
			}
			var root telegramRoot
			if err := json.Unmarshal(document.Root(), &root); err != nil {
				t.Fatal(err)
			}
			seen[root.BotID] = root
		case <-time.After(2 * time.Second):
			t.Fatalf("got updates for bots %#v, want main and ops", seen)
		}
	}

	assertTelegramRoot(t, seen["main"], 100, "message", 11, 7)
	assertTelegramRoot(t, seen["ops"], 200, "callback_query", 12, 8)
	for _, root := range seen {
		raw, _ := json.Marshal(root)
		if strings.Contains(string(raw), "token-main") || strings.Contains(string(raw), "token-ops") {
			t.Fatalf("event contains a Telegram token: %s", raw)
		}
	}

	cancel()
	select {
	case err := <-runError:
		if err != nil {
			t.Fatalf("graceful cancellation returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Telegram listener did not stop after context cancellation")
	}
}

func TestListenerRetriesBackpressuredUpdateWithoutAdvancingOffset(t *testing.T) {
	var mu sync.Mutex
	var offsets []string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if err := request.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
		}
		mu.Lock()
		offsets = append(offsets, request.Form.Get("offset"))
		mu.Unlock()
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"ok":true,"result":[{"update_id":321,"message":{"chat":{"id":1}}}]}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var submitMu sync.Mutex
	submits := 0
	telegramListener, err := New(Config{
		Bots:       []Bot{{ID: "retry", Token: "retry-token", APIBase: server.URL, PollTimeout: time.Second}},
		RetryDelay: time.Millisecond,
	}, listener.SinkFunc(func(_ context.Context, _ event.Document) error {
		submitMu.Lock()
		defer submitMu.Unlock()
		submits++
		if submits == 1 {
			return listener.ErrBackpressure
		}
		cancel()
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := telegramListener.Run(ctx); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(offsets) < 2 || offsets[0] != "0" || offsets[1] != "0" {
		t.Fatalf("offsets = %#v, want the rejected update requested again", offsets)
	}
}

func TestListenerRedactsTokenFromFatalAPIError(t *testing.T) {
	const token = "highly-secret-token"
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusUnauthorized)
		_, _ = fmt.Fprintf(response, `{"ok":false,"error_code":401,"description":"invalid %s"}`, token)
	}))
	defer server.Close()

	telegramListener, err := New(Config{
		Bots: []Bot{{ID: "private", Token: token, APIBase: server.URL}},
	}, listener.SinkFunc(func(context.Context, event.Document) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	err = telegramListener.Run(context.Background())
	if err == nil {
		t.Fatal("expected authentication error")
	}
	if strings.Contains(err.Error(), token) {
		t.Fatalf("error leaked bot token: %v", err)
	}
}

type telegramRoot struct {
	Source        string         `json:"source"`
	SchemaVersion int            `json:"schema_version"`
	EventID       string         `json:"event_id"`
	ReceivedAt    string         `json:"received_at"`
	BotID         string         `json:"bot_id"`
	UpdateID      int64          `json:"update_id"`
	UpdateType    string         `json:"update_type"`
	ChatID        *int64         `json:"chat_id"`
	UserID        *int64         `json:"user_id"`
	Telegram      map[string]any `json:"telegram"`
}

func assertTelegramRoot(t *testing.T, root telegramRoot, updateID int64, updateType string, chatID, userID int64) {
	t.Helper()
	if root.Source != "telegram" || root.SchemaVersion != 1 || root.EventID == "" || root.ReceivedAt == "" {
		t.Fatalf("invalid common fields: %#v", root)
	}
	if root.UpdateID != updateID || root.UpdateType != updateType {
		t.Fatalf("invalid update fields: %#v", root)
	}
	if root.ChatID == nil || *root.ChatID != chatID || root.UserID == nil || *root.UserID != userID {
		t.Fatalf("invalid filter fields: chat=%v user=%v", root.ChatID, root.UserID)
	}
	if _, exists := root.Telegram["update"]; !exists {
		t.Fatal("telegram.update is missing")
	}
}

func tokenFromPath(t *testing.T, path string) string {
	t.Helper()
	if !strings.HasPrefix(path, "/bot") || !strings.HasSuffix(path, "/getUpdates") {
		t.Fatalf("unexpected Telegram API path %q", path)
	}
	return strings.TrimSuffix(strings.TrimPrefix(path, "/bot"), "/getUpdates")
}
