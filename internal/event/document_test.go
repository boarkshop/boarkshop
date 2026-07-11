package event

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestGenerateID(t *testing.T) {
	first, err := GenerateID()
	if err != nil {
		t.Fatalf("GenerateID() error = %v", err)
	}
	second, err := GenerateID()
	if err != nil {
		t.Fatalf("GenerateID() error = %v", err)
	}
	if first == second {
		t.Fatalf("two generated IDs are equal: %q", first)
	}
	if len(first) != 2*IDBytes {
		t.Fatalf("ID length = %d", len(first))
	}
	if _, err := hex.DecodeString(first); err != nil {
		t.Fatalf("ID %q is not hex: %v", first, err)
	}
}

func TestNewStoresCompleteRootWithoutPayloadWrapper(t *testing.T) {
	document, err := New("http", 1, map[string]any{
		"method": "POST",
		"path":   "/webhooks/github",
		"request": map[string]any{
			"body": map[string]any{"action": "opened"},
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var root map[string]json.RawMessage
	if err := json.Unmarshal(document.Root(), &root); err != nil {
		t.Fatalf("Unmarshal(root): %v", err)
	}
	for _, field := range []string{"source", "schema_version", "event_id", "received_at", "method", "path", "request"} {
		if _, exists := root[field]; !exists {
			t.Errorf("root field %q is absent", field)
		}
	}
	if _, exists := root["payload"]; exists {
		t.Error("constructor added a payload wrapper")
	}

	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatalf("json.Marshal(Document): %v", err)
	}
	if string(encoded) != string(document.Root()) {
		t.Errorf("MarshalJSON() = %s, root = %s", encoded, document.Root())
	}
}

func TestNewWithMetadataAndParse(t *testing.T) {
	receivedAt := time.Date(2026, time.July, 11, 12, 0, 0, 123, time.FixedZone("offset", 4*60*60))
	document, err := NewWithMetadata(
		"telegram",
		1,
		"0123456789abcdef0123456789abcdef",
		receivedAt,
		map[string]any{"chat_id": 123},
	)
	if err != nil {
		t.Fatalf("NewWithMetadata() error = %v", err)
	}
	if !document.ReceivedAt.Equal(receivedAt) || document.ReceivedAt.Location() != time.UTC {
		t.Errorf("ReceivedAt = %v", document.ReceivedAt)
	}

	parsed, err := Parse(document.Root())
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if parsed.Source != "telegram" || parsed.EventID != document.EventID || parsed.SchemaVersion != 1 {
		t.Errorf("parsed metadata = %+v", parsed)
	}
}

func TestRootReturnsCopy(t *testing.T) {
	document, err := New("cron", 1, map[string]any{"schedule_id": "hourly"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	first := document.Root()
	first[0] = '!'
	if document.Root()[0] == '!' {
		t.Error("Root() exposed mutable internal storage")
	}
}

func TestNewRejectsReservedFields(t *testing.T) {
	_, err := New("http", 1, map[string]any{"event_id": "caller-value"})
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("New() error = %v", err)
	}
}

func TestParseValidation(t *testing.T) {
	validID := "0123456789abcdef0123456789abcdef"
	tests := []struct {
		name string
		json string
		want string
	}{
		{name: "array", json: `[]`, want: "JSON object"},
		{name: "missing source", json: `{"schema_version":1,"event_id":"` + validID + `","received_at":"2026-07-11T12:00:00Z"}`, want: `"source" is required`},
		{name: "bad schema", json: `{"source":"http","schema_version":0,"event_id":"` + validID + `","received_at":"2026-07-11T12:00:00Z"}`, want: "schema_version must be positive"},
		{name: "bad id", json: `{"source":"http","schema_version":1,"event_id":"not-an-id","received_at":"2026-07-11T12:00:00Z"}`, want: "event_id must be"},
		{name: "zero time", json: `{"source":"http","schema_version":1,"event_id":"` + validID + `","received_at":"0001-01-01T00:00:00Z"}`, want: "received_at must not be zero"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse([]byte(test.json))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Parse() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestJSONUnmarshalValidatesDocument(t *testing.T) {
	raw := []byte(`{"source":"cron","schema_version":1,"event_id":"0123456789abcdef0123456789abcdef","received_at":"2026-07-11T12:00:00Z","schedule_id":"daily"}`)
	var document Document
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("json.Unmarshal(Document): %v", err)
	}
	if document.Source != "cron" || string(document.Root()) != string(raw) {
		t.Errorf("document = %+v, root=%s", document, document.Root())
	}
}
