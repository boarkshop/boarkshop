package dispatch

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/boarkshop/boarkshop/internal/event"
)

func TestDispatcherBackpressureAndDrain(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	var handled atomic.Int32

	d, err := New(1, 1, HandlerFunc(func(context.Context, event.Document) {
		if handled.Add(1) == 1 {
			close(started)
		}
		<-release
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}

	doc := event.Document{}
	if err := d.Submit(context.Background(), doc); err != nil {
		t.Fatalf("submit first event: %v", err)
	}
	<-started
	if err := d.Submit(context.Background(), doc); err != nil {
		t.Fatalf("submit queued event: %v", err)
	}
	if err := d.Submit(context.Background(), doc); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := d.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := handled.Load(); got != 2 {
		t.Fatalf("handled %d events, want 2", got)
	}
	if err := d.Submit(context.Background(), doc); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestDispatcherForcedShutdownCancelsHandler(t *testing.T) {
	started := make(chan struct{})
	d, err := New(1, 1, HandlerFunc(func(ctx context.Context, _ event.Document) {
		close(started)
		<-ctx.Done()
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	if err := d.Submit(context.Background(), event.Document{}); err != nil {
		t.Fatal(err)
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := d.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got %v", err)
	}
}
