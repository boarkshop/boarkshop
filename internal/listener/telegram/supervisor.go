package telegram

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/boarkshop/boarkshop/internal/listener"
)

// BotSource returns the complete desired set of Telegram bots. A load error
// must describe an unusable generation rather than a partially usable set.
type BotSource interface {
	Load() ([]Bot, error)
}

type SupervisorConfig struct {
	Initial        []Bot
	Source         BotSource
	ReloadInterval time.Duration
	RetryDelay     time.Duration
	HTTPClient     *http.Client
	Logger         *slog.Logger
}

// Supervisor reconciles a changing bot catalog into independent pollers. A
// poller failure is isolated to its bot and never terminates the supervisor.
type Supervisor struct {
	initial        []Bot
	source         BotSource
	reloadInterval time.Duration
	retryDelay     time.Duration
	client         *http.Client
	sink           listener.Sink
	logger         *slog.Logger

	mu      sync.Mutex
	running bool
}

type botWorker struct {
	bot    Bot
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

type supervisorRuntime struct {
	workers    map[string]*botWorker
	failed     map[string]Bot
	exitWakeup chan struct{}
}

func NewSupervisor(config SupervisorConfig, sink listener.Sink) (*Supervisor, error) {
	if sink == nil {
		return nil, fmt.Errorf("event sink is required")
	}
	if config.Source == nil {
		return nil, fmt.Errorf("Telegram bot source is required")
	}
	if config.ReloadInterval == 0 {
		config.ReloadInterval = time.Second
	}
	if config.ReloadInterval < 0 {
		return nil, fmt.Errorf("Telegram reload interval cannot be negative")
	}
	normalized, err := normalizeConfig(Config{
		Bots:       config.Initial,
		RetryDelay: config.RetryDelay,
		HTTPClient: config.HTTPClient,
	}, true)
	if err != nil {
		return nil, err
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Supervisor{
		initial:        normalized.Bots,
		source:         config.Source,
		reloadInterval: config.ReloadInterval,
		retryDelay:     normalized.RetryDelay,
		client:         normalized.HTTPClient,
		sink:           sink,
		logger:         config.Logger,
	}, nil
}

// Run starts the initial generation and keeps reconciling until ctx is
// canceled. The same Supervisor cannot be run concurrently.
func (s *Supervisor) Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("context is required")
	}
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("Telegram supervisor is already running")
	}
	s.running = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()

	runtime := &supervisorRuntime{
		workers:    make(map[string]*botWorker),
		failed:     make(map[string]Bot),
		exitWakeup: make(chan struct{}, 1),
	}
	if err := s.reconcile(ctx, runtime, s.initial); err != nil {
		s.shutdown(runtime)
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	ticker := time.NewTicker(s.reloadInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.shutdown(runtime)
			return nil
		case <-runtime.exitWakeup:
			s.harvestExited(runtime)
		case <-ticker.C:
			bots, err := s.source.Load()
			if err != nil {
				s.logger.Error("Telegram bot catalog reload failed", "error", err, "using_last_good", true)
				continue
			}
			if err := s.reconcile(ctx, runtime, bots); err != nil {
				if ctx.Err() != nil {
					s.shutdown(runtime)
					return nil
				}
				s.logger.Error("Telegram bot catalog apply failed", "error", err, "using_last_good", true)
			}
		}
	}
}

func (s *Supervisor) Start(ctx context.Context) error {
	return s.Run(ctx)
}

func (s *Supervisor) reconcile(ctx context.Context, runtime *supervisorRuntime, bots []Bot) error {
	normalized, err := normalizeConfig(Config{
		Bots:       bots,
		RetryDelay: s.retryDelay,
		HTTPClient: s.client,
	}, true)
	if err != nil {
		return err
	}
	s.harvestExited(runtime)

	desired := make(map[string]Bot, len(normalized.Bots))
	for _, bot := range normalized.Bots {
		desired[bot.ID] = bot
	}

	// Stop removals and replacements before starting additions. This prevents
	// overlapping getUpdates calls when a token moves between IDs.
	for id, worker := range runtime.workers {
		bot, exists := desired[id]
		if exists && sameBot(worker.bot, bot) {
			continue
		}
		worker.cancel()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-worker.done:
		}
		delete(runtime.workers, id)
		delete(runtime.failed, id)
		if exists {
			s.logger.Info("Telegram bot stopped for replacement", "bot_id", id)
		} else {
			s.logger.Info("Telegram bot removed", "bot_id", id)
		}
	}

	for id, failedBot := range runtime.failed {
		bot, exists := desired[id]
		if !exists || !sameBot(failedBot, bot) {
			delete(runtime.failed, id)
		}
	}
	for _, bot := range normalized.Bots {
		if _, exists := runtime.workers[bot.ID]; exists {
			continue
		}
		if failedBot, failed := runtime.failed[bot.ID]; failed && sameBot(failedBot, bot) {
			continue
		}
		if err := s.startWorker(ctx, runtime, bot); err != nil {
			runtime.failed[bot.ID] = bot
			s.logger.Error("Telegram bot failed to start", "bot_id", bot.ID, "error", err)
			continue
		}
		s.logger.Info("Telegram bot started", "bot_id", bot.ID)
	}
	return nil
}

func (s *Supervisor) startWorker(ctx context.Context, runtime *supervisorRuntime, bot Bot) error {
	poller, err := New(Config{
		Bots:       []Bot{bot},
		RetryDelay: s.retryDelay,
		HTTPClient: s.client,
	}, s.sink)
	if err != nil {
		return err
	}
	workerCtx, cancel := context.WithCancel(ctx)
	worker := &botWorker{
		bot:    bot,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	runtime.workers[bot.ID] = worker
	go func() {
		worker.err = poller.Run(workerCtx)
		close(worker.done)
		select {
		case runtime.exitWakeup <- struct{}{}:
		default:
		}
	}()
	return nil
}

func (s *Supervisor) harvestExited(runtime *supervisorRuntime) {
	for id, worker := range runtime.workers {
		select {
		case <-worker.done:
			delete(runtime.workers, id)
			runtime.failed[id] = worker.bot
			if worker.err != nil {
				s.logger.Error("Telegram bot stopped with an error", "bot_id", id, "error", worker.err)
			} else {
				s.logger.Warn("Telegram bot stopped unexpectedly", "bot_id", id)
			}
		default:
		}
	}
}

func (s *Supervisor) shutdown(runtime *supervisorRuntime) {
	for _, worker := range runtime.workers {
		worker.cancel()
	}
	for _, worker := range runtime.workers {
		<-worker.done
	}
}

func sameBot(left, right Bot) bool {
	return left.ID == right.ID &&
		left.Token == right.Token &&
		left.APIBase == right.APIBase &&
		left.PollTimeout == right.PollTimeout
}
