// Package http implements the generic Boarkshop HTTP event listener.
package http

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/boarkshop/boarkshop/internal/listener"
)

const (
	// DefaultMaxBodyBytes is deliberately conservative. Deployments that need
	// larger webhook payloads can opt in through Config.MaxBodyBytes.
	DefaultMaxBodyBytes    int64 = 1 << 20
	defaultAddress               = "127.0.0.1:8080"
	defaultShutdownTimeout       = 5 * time.Second
)

// Config configures the generic HTTP listener.
type Config struct {
	Address           string
	MaxBodyBytes      int64
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
}

// Listener accepts all methods and paths and normalizes requests into events.
type Listener struct {
	config Config
	sink   listener.Sink
	server *stdhttp.Server

	mu       sync.RWMutex
	listener net.Listener
	started  bool
}

// New constructs a listener. Call Run, Start, or Serve to bind it.
func New(config Config, sink listener.Sink) (*Listener, error) {
	if sink == nil {
		return nil, fmt.Errorf("event sink is required")
	}
	if config.Address == "" {
		config.Address = defaultAddress
	}
	if config.MaxBodyBytes == 0 {
		config.MaxBodyBytes = DefaultMaxBodyBytes
	}
	if config.MaxBodyBytes < 0 {
		return nil, fmt.Errorf("max body bytes cannot be negative")
	}
	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = defaultShutdownTimeout
	}
	if config.ShutdownTimeout < 0 {
		return nil, fmt.Errorf("shutdown timeout cannot be negative")
	}

	l := &Listener{config: config, sink: sink}
	l.server = &stdhttp.Server{
		Addr:              config.Address,
		Handler:           stdhttp.HandlerFunc(l.handle),
		ReadHeaderTimeout: config.ReadHeaderTimeout,
		ReadTimeout:       config.ReadTimeout,
		WriteTimeout:      config.WriteTimeout,
		IdleTimeout:       config.IdleTimeout,
	}
	return l, nil
}

// Handler returns the listener's generic HTTP handler. It is primarily useful
// for embedding and transport-level tests; it has the same behaviour as Run.
func (l *Listener) Handler() stdhttp.Handler {
	return l.server.Handler
}

// Run binds Config.Address and serves until ctx is canceled.
func (l *Listener) Run(ctx context.Context) error {
	return l.Start(ctx)
}

// Start binds Config.Address and serves until ctx is canceled.
func (l *Listener) Start(ctx context.Context) error {
	var listenConfig net.ListenConfig
	networkListener, err := listenConfig.Listen(ctx, "tcp", l.config.Address)
	if err != nil {
		return fmt.Errorf("bind HTTP listener: %w", err)
	}
	return l.Serve(ctx, networkListener)
}

// Serve serves an already-bound network listener until ctx is canceled.
func (l *Listener) Serve(ctx context.Context, networkListener net.Listener) error {
	if networkListener == nil {
		return fmt.Errorf("network listener is required")
	}

	l.mu.Lock()
	if l.started {
		l.mu.Unlock()
		_ = networkListener.Close()
		return fmt.Errorf("HTTP listener already started")
	}
	l.started = true
	l.listener = networkListener
	l.mu.Unlock()

	serveDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), l.config.ShutdownTimeout)
			defer cancel()
			if err := l.server.Shutdown(shutdownCtx); err != nil {
				_ = l.server.Close()
			}
		case <-serveDone:
		}
	}()

	err := l.server.Serve(networkListener)
	close(serveDone)
	if errors.Is(err, stdhttp.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown gracefully stops a running listener.
func (l *Listener) Shutdown(ctx context.Context) error {
	return l.server.Shutdown(ctx)
}

// Addr returns the bound address after Start or Serve has begun.
func (l *Listener) Addr() net.Addr {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.listener == nil {
		return nil
	}
	return l.listener.Addr()
}

func (l *Listener) handle(response stdhttp.ResponseWriter, request *stdhttp.Request) {
	receivedAt := time.Now().UTC()
	defer request.Body.Close()

	limitedBody := stdhttp.MaxBytesReader(response, request.Body, l.config.MaxBodyBytes)
	body, err := io.ReadAll(limitedBody)
	if err != nil {
		var tooLarge *stdhttp.MaxBytesError
		if errors.As(err, &tooLarge) {
			stdhttp.Error(response, "request body too large", stdhttp.StatusRequestEntityTooLarge)
			return
		}
		stdhttp.Error(response, "cannot read request body", stdhttp.StatusBadRequest)
		return
	}

	headers := cloneValues(request.Header)
	if request.Host != "" {
		headers["Host"] = []string{request.Host}
	}
	requestFields := map[string]any{
		"query":          cloneValues(request.URL.Query()),
		"headers":        headers,
		"body_base64":    base64.StdEncoding.EncodeToString(body),
		"remote_address": remoteAddress(request.RemoteAddr),
	}
	if utf8.Valid(body) {
		requestFields["body_text"] = string(body)
	}
	if bodyJSON, ok := decodeJSON(body); ok {
		requestFields["body_json"] = bodyJSON
	}

	document, err := listener.NewDocument("http", receivedAt, map[string]any{
		"method":  request.Method,
		"path":    request.URL.Path,
		"request": requestFields,
	})
	if err != nil {
		stdhttp.Error(response, "cannot create event", stdhttp.StatusInternalServerError)
		return
	}
	if err := l.sink.Submit(request.Context(), document); err != nil {
		stdhttp.Error(response, "event queue unavailable", stdhttp.StatusServiceUnavailable)
		return
	}

	response.WriteHeader(stdhttp.StatusAccepted)
}

func decodeJSON(body []byte) (any, bool) {
	if len(bytes.TrimSpace(body)) == 0 || !json.Valid(body) {
		return nil, false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, false
	}
	return value, true
}

func cloneValues(values map[string][]string) map[string][]string {
	clone := make(map[string][]string, len(values))
	for key, value := range values {
		clone[key] = append([]string(nil), value...)
	}
	return clone
}

func remoteAddress(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err == nil {
		return host
	}
	return strings.TrimSpace(address)
}
