package event

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const IDBytes = 16

var reservedFields = map[string]struct{}{
	"source":         {},
	"schema_version": {},
	"event_id":       {},
	"received_at":    {},
}

// Document contains validated event metadata and the complete listener-specific
// root JSON object. MarshalJSON emits that object directly; it never adds a
// payload envelope.
type Document struct {
	Source        string
	SchemaVersion int
	EventID       string
	ReceivedAt    time.Time
	root          json.RawMessage
}

// GenerateID returns a cryptographically random 128-bit lowercase hex ID.
func GenerateID() (string, error) {
	bytes := make([]byte, IDBytes)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate event id: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

// New creates an event with a fresh ID and the current UTC timestamp. Fields
// are placed directly at the JSON root alongside the four metadata fields.
func New(source string, schemaVersion int, fields map[string]any) (*Document, error) {
	eventID, err := GenerateID()
	if err != nil {
		return nil, err
	}
	return NewWithMetadata(source, schemaVersion, eventID, time.Now().UTC(), fields)
}

// NewWithMetadata is the deterministic, validating constructor used by
// listeners that already have an ID or receive timestamp.
func NewWithMetadata(source string, schemaVersion int, eventID string, receivedAt time.Time, fields map[string]any) (*Document, error) {
	if err := validateMetadata(source, schemaVersion, eventID, receivedAt); err != nil {
		return nil, err
	}

	root := make(map[string]any, len(fields)+len(reservedFields))
	for name, value := range fields {
		if _, reserved := reservedFields[name]; reserved {
			return nil, fmt.Errorf("event field %q is reserved", name)
		}
		root[name] = value
	}
	root["source"] = source
	root["schema_version"] = schemaVersion
	root["event_id"] = eventID
	root["received_at"] = receivedAt.UTC().Format(time.RFC3339Nano)

	raw, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("encode event root: %w", err)
	}
	return Parse(raw)
}

// Parse validates an existing complete root JSON event document.
func Parse(raw []byte) (*Document, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, fmt.Errorf("event root must be a JSON object: %w", err)
	}
	if root == nil {
		return nil, fmt.Errorf("event root must be a JSON object")
	}

	var source string
	if err := requiredField(root, "source", &source); err != nil {
		return nil, err
	}
	var schemaVersion int
	if err := requiredField(root, "schema_version", &schemaVersion); err != nil {
		return nil, err
	}
	var eventID string
	if err := requiredField(root, "event_id", &eventID); err != nil {
		return nil, err
	}
	var receivedAt time.Time
	if err := requiredField(root, "received_at", &receivedAt); err != nil {
		return nil, err
	}
	if err := validateMetadata(source, schemaVersion, eventID, receivedAt); err != nil {
		return nil, err
	}

	copyOfRoot := append(json.RawMessage(nil), raw...)
	return &Document{
		Source:        source,
		SchemaVersion: schemaVersion,
		EventID:       eventID,
		ReceivedAt:    receivedAt.UTC(),
		root:          copyOfRoot,
	}, nil
}

// Root returns a copy of the full root JSON document.
func (document Document) Root() json.RawMessage {
	return append(json.RawMessage(nil), document.root...)
}

func (document Document) MarshalJSON() ([]byte, error) {
	if len(document.root) == 0 || !json.Valid(document.root) {
		return nil, fmt.Errorf("event document has no valid root JSON")
	}
	return document.Root(), nil
}

func (document *Document) UnmarshalJSON(raw []byte) error {
	parsed, err := Parse(raw)
	if err != nil {
		return err
	}
	*document = *parsed
	return nil
}

func requiredField(root map[string]json.RawMessage, name string, target any) error {
	raw, exists := root[name]
	if !exists {
		return fmt.Errorf("event field %q is required", name)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("event field %q is invalid: %w", name, err)
	}
	return nil
}

func validateMetadata(source string, schemaVersion int, eventID string, receivedAt time.Time) error {
	if source == "" || source != strings.TrimSpace(source) {
		return fmt.Errorf("event source must be non-empty and trimmed")
	}
	if schemaVersion < 1 {
		return fmt.Errorf("event schema_version must be positive")
	}
	if len(eventID) != hex.EncodedLen(IDBytes) {
		return fmt.Errorf("event_id must be a %d-character hex string", hex.EncodedLen(IDBytes))
	}
	if _, err := hex.DecodeString(eventID); err != nil {
		return fmt.Errorf("event_id must be hexadecimal: %w", err)
	}
	if receivedAt.IsZero() {
		return fmt.Errorf("event received_at must not be zero")
	}
	return nil
}
