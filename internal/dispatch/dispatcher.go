package dispatch

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/boarkshop/boarkshop/internal/event"
)

var (
	// ErrQueueFull is returned when an event cannot be accepted without blocking.
	ErrQueueFull = errors.New("event queue is full")
	// ErrClosed is returned after the dispatcher stopped accepting events.
	ErrClosed = errors.New("dispatcher is closed")
)

// Handler consumes one event. Implementations must respect ctx so a forced
// shutdown can terminate outstanding work.
type Handler interface {
	Handle(context.Context, event.Document)
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, event.Document)

func (f HandlerFunc) Handle(ctx context.Context, document event.Document) {
	f(ctx, document)
}

// Dispatcher is a bounded, in-memory event queue. Submit is intentionally
// non-blocking: transports decide how to expose backpressure to their callers.
type Dispatcher struct {
	queue   chan event.Document
	workers int
	handler Handler

	mu      sync.RWMutex
	started bool
	closed  bool

	runCtx context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	done   chan struct{}
}

func New(capacity, workers int, handler Handler) (*Dispatcher, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("queue capacity must be greater than zero")
	}
	if workers <= 0 {
		return nil, fmt.Errorf("worker count must be greater than zero")
	}
	if handler == nil {
		return nil, fmt.Errorf("handler is required")
	}

	runCtx, cancel := context.WithCancel(context.Background())
	return &Dispatcher{
		queue:   make(chan event.Document, capacity),
		workers: workers,
		handler: handler,
		runCtx:  runCtx,
		cancel:  cancel,
		done:    make(chan struct{}),
	}, nil
}

// Start launches the fixed worker pool. A dispatcher may be started once.
func (d *Dispatcher) Start() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.started {
		return fmt.Errorf("dispatcher already started")
	}
	if d.closed {
		return ErrClosed
	}
	d.started = true
	for range d.workers {
		d.wg.Add(1)
		go d.worker()
	}
	return nil
}

// Submit accepts document only when queue capacity is immediately available.
func (d *Dispatcher) Submit(ctx context.Context, document event.Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return ErrClosed
	}
	if !d.started {
		return fmt.Errorf("dispatcher is not started")
	}

	select {
	case d.queue <- document:
		return nil
	default:
		return ErrQueueFull
	}
}

// Close stops accepting events and waits for all accepted work to finish. If
// ctx expires, outstanding handlers are canceled and Close returns ctx.Err().
func (d *Dispatcher) Close(ctx context.Context) error {
	d.mu.Lock()
	if !d.closed {
		d.closed = true
		close(d.queue)
		go func() {
			d.wg.Wait()
			close(d.done)
		}()
	}
	d.mu.Unlock()

	select {
	case <-d.done:
		d.cancel()
		return nil
	case <-ctx.Done():
		d.cancel()
		<-d.done
		return ctx.Err()
	}
}

func (d *Dispatcher) worker() {
	defer d.wg.Done()
	for document := range d.queue {
		if d.runCtx.Err() != nil {
			return
		}
		d.handler.Handle(d.runCtx, document)
	}
}
