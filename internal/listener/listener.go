// Package listener defines the boundary between event listeners and the
// daemon's event dispatcher.
package listener

import (
	"context"
	"errors"
	"time"

	"github.com/boarkshop/boarkshop/internal/event"
)

// SchemaVersion is the first stable schema version shared by the MVP listener
// event contracts. Each document still carries its source-specific root.
const SchemaVersion = 1

// ErrBackpressure can be returned by a Sink when it cannot accept an event
// immediately. Submit implementations must not wait for queue capacity: the
// listener owns the transport-specific response or retry behaviour.
var ErrBackpressure = errors.New("event sink is at capacity")

// Sink accepts normalized listener events. Submit is a non-blocking admission
// operation; successful processing of the pipeline is intentionally outside
// this contract.
type Sink interface {
	Submit(context.Context, event.Document) error
}

// SinkFunc adapts a function to Sink.
type SinkFunc func(context.Context, event.Document) error

// Submit implements Sink.
func (f SinkFunc) Submit(ctx context.Context, document event.Document) error {
	return f(ctx, document)
}

// NewDocument creates a value-form event document for submission to a Sink.
func NewDocument(source string, receivedAt time.Time, fields map[string]any) (event.Document, error) {
	eventID, err := event.GenerateID()
	if err != nil {
		return event.Document{}, err
	}
	document, err := event.NewWithMetadata(source, SchemaVersion, eventID, receivedAt, fields)
	if err != nil {
		return event.Document{}, err
	}
	return *document, nil
}
