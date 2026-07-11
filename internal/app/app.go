package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/boarkshop/boarkshop/internal/config"
	"github.com/boarkshop/boarkshop/internal/dispatch"
	"github.com/boarkshop/boarkshop/internal/engine"
	"github.com/boarkshop/boarkshop/internal/event"
	"github.com/boarkshop/boarkshop/internal/listener"
	cronlistener "github.com/boarkshop/boarkshop/internal/listener/cron"
	httplistener "github.com/boarkshop/boarkshop/internal/listener/http"
	telegramlistener "github.com/boarkshop/boarkshop/internal/listener/telegram"
	processrun "github.com/boarkshop/boarkshop/internal/process"
	"github.com/boarkshop/boarkshop/internal/storage"
)

type definition struct {
	instance  *config.Instance
	pipelines []engine.Pipeline
	bots      []resolvedBot
}

type resolvedBot struct {
	ID          string
	Token       string
	APIBase     string
	PollTimeout config.Duration
}

type component struct {
	name  string
	start func(context.Context) error
}

// Validate loads every static runtime input and constructs all listeners
// without binding sockets or starting network requests.
func Validate(configPath string) error {
	loaded, err := loadDefinition(configPath)
	if err != nil {
		return err
	}
	_, err = makeComponents(loaded, listener.SinkFunc(func(context.Context, event.Document) error { return nil }))
	return err
}

// Run starts a configured single-process daemon and blocks until ctx is
// canceled or a listener fails.
func Run(ctx context.Context, configPath string, logger *slog.Logger) error {
	if ctx == nil {
		return fmt.Errorf("context is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	loaded, err := loadDefinition(configPath)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(loaded.instance.PipelinesDir, 0o700); err != nil {
		return fmt.Errorf("create pipelines directory: %w", err)
	}
	layout, err := storage.Prepare(loaded.instance.DataDir)
	if err != nil {
		return err
	}
	if err := layout.CleanupRuns(); err != nil {
		return err
	}

	executor, err := engine.New(
		loaded.pipelines,
		processrun.Runner{},
		layout,
		loaded.instance.MaxParallelProcesses,
		logger,
	)
	if err != nil {
		return fmt.Errorf("create pipeline executor: %w", err)
	}
	dispatcher, err := dispatch.New(
		loaded.instance.QueueSize,
		loaded.instance.MaxParallelProcesses,
		executor,
	)
	if err != nil {
		return fmt.Errorf("create dispatcher: %w", err)
	}
	if err := dispatcher.Start(); err != nil {
		return fmt.Errorf("start dispatcher: %w", err)
	}

	sink := dispatcherSink{dispatcher: dispatcher, logger: logger}
	components, err := makeComponents(loaded, sink)
	if err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), loaded.instance.ShutdownTimeout.Std())
		defer cancel()
		_ = dispatcher.Close(shutdownCtx)
		return err
	}

	logger.Warn("volatile event delivery enabled",
		"delivery_guarantee", "best_effort",
		"queue_size", loaded.instance.QueueSize,
		"message", "accepted events can be lost if the process exits unexpectedly",
	)
	logger.Info("boarkshop started",
		"pipelines", len(loaded.pipelines),
		"listeners", len(components),
		"max_parallel_processes", loaded.instance.MaxParallelProcesses,
	)

	runCtx, cancelRun := context.WithCancel(context.Background())
	var componentWG sync.WaitGroup
	componentErrors := make(chan error, max(1, len(components)))
	for _, item := range components {
		item := item
		componentWG.Add(1)
		go func() {
			defer componentWG.Done()
			logger.Info("listener started", "listener", item.name)
			if err := item.start(runCtx); err != nil && runCtx.Err() == nil {
				select {
				case componentErrors <- fmt.Errorf("listener %s: %w", item.name, err):
				default:
				}
			}
			logger.Info("listener stopped", "listener", item.name)
		}()
	}
	componentsDone := make(chan struct{})
	go func() {
		componentWG.Wait()
		close(componentsDone)
	}()
	var unexpectedStop <-chan struct{}
	if len(components) > 0 {
		unexpectedStop = componentsDone
	}

	var runErr error
	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			runErr = ctx.Err()
		}
	case runErr = <-componentErrors:
	case <-unexpectedStop:
		select {
		case runErr = <-componentErrors:
		default:
			runErr = fmt.Errorf("all listeners stopped unexpectedly")
		}
	}
	cancelRun()
	<-componentsDone

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), loaded.instance.ShutdownTimeout.Std())
	defer cancelShutdown()
	closeErr := dispatcher.Close(shutdownCtx)
	if closeErr != nil {
		logger.Error("dispatcher shutdown deadline exceeded", "error", closeErr)
		if runErr == nil {
			runErr = closeErr
		}
	}
	logger.Info("boarkshop stopped")
	return runErr
}

func loadDefinition(configPath string) (*definition, error) {
	instance, err := config.LoadInstance(configPath)
	if err != nil {
		return nil, err
	}

	manifests, err := loadPipelineManifests(instance.PipelinesDir)
	if err != nil {
		return nil, err
	}
	pipelines := make([]engine.Pipeline, 0, len(manifests))
	for _, manifest := range manifests {
		if !manifest.Enabled {
			continue
		}
		secretEnv := make(map[string]string, len(manifest.Secrets))
		for destination, reference := range manifest.Secrets {
			value, err := resolveReference(reference.Env, reference.File)
			if err != nil {
				return nil, fmt.Errorf("pipeline %q secret %q: %w", manifest.ID, destination, err)
			}
			secretEnv[destination] = value
		}
		steps := make([]engine.Step, 0, len(manifest.Steps))
		for _, step := range manifest.Steps {
			steps = append(steps, engine.Step{ID: step.ID, Argv: append([]string(nil), step.Argv...), Timeout: step.Timeout.Std()})
		}
		pipelines = append(pipelines, engine.Pipeline{
			ID:        manifest.ID,
			Directory: manifest.Dir,
			Env:       cloneStringMap(manifest.Env),
			SecretEnv: secretEnv,
			Guard: engine.Step{
				ID:      "guard",
				Argv:    append([]string(nil), manifest.Guard.Argv...),
				Timeout: manifest.Guard.Timeout.Std(),
			},
			Steps: steps,
		})
	}

	bots := make([]resolvedBot, 0, len(instance.Listeners.Telegram.Bots))
	for _, bot := range instance.Listeners.Telegram.Bots {
		token, err := resolveReference(bot.Token.Env, bot.Token.File)
		if err != nil {
			return nil, fmt.Errorf("Telegram bot %q token: %w", bot.ID, err)
		}
		if token == "" {
			return nil, fmt.Errorf("Telegram bot %q token is empty", bot.ID)
		}
		bots = append(bots, resolvedBot{
			ID:          bot.ID,
			Token:       token,
			APIBase:     bot.APIBase,
			PollTimeout: bot.PollTimeout,
		})
	}
	return &definition{instance: instance, pipelines: pipelines, bots: bots}, nil
}

func loadPipelineManifests(root string) ([]*config.Pipeline, error) {
	info, err := os.Stat(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect pipelines directory %q: %w", root, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("pipelines path %q is not a directory", root)
	}
	return config.LoadPipelines(root)
}

func makeComponents(loaded *definition, sink listener.Sink) ([]component, error) {
	components := make([]component, 0, 3)
	if loaded.instance.Listeners.HTTP.Enabled {
		configured := loaded.instance.Listeners.HTTP
		httpServer, err := httplistener.New(httplistener.Config{
			Address:           configured.Address,
			MaxBodyBytes:      configured.MaxBodyBytes,
			ReadHeaderTimeout: configured.ReadHeaderTimeout.Std(),
			ShutdownTimeout:   loaded.instance.ShutdownTimeout.Std(),
		}, sink)
		if err != nil {
			return nil, fmt.Errorf("configure HTTP listener: %w", err)
		}
		components = append(components, component{name: "http", start: httpServer.Start})
	}
	if len(loaded.bots) > 0 {
		bots := make([]telegramlistener.Bot, 0, len(loaded.bots))
		for _, bot := range loaded.bots {
			bots = append(bots, telegramlistener.Bot{
				ID:          bot.ID,
				Token:       bot.Token,
				APIBase:     bot.APIBase,
				PollTimeout: bot.PollTimeout.Std(),
			})
		}
		telegram, err := telegramlistener.New(telegramlistener.Config{Bots: bots}, sink)
		if err != nil {
			return nil, fmt.Errorf("configure Telegram listener: %w", err)
		}
		components = append(components, component{name: "telegram", start: telegram.Start})
	}
	if len(loaded.instance.Listeners.Cron.Schedules) > 0 {
		schedules := make([]cronlistener.Schedule, 0, len(loaded.instance.Listeners.Cron.Schedules))
		for _, schedule := range loaded.instance.Listeners.Cron.Schedules {
			schedules = append(schedules, cronlistener.Schedule{ID: schedule.ID, Expression: schedule.Expression})
		}
		cron, err := cronlistener.New(cronlistener.Config{
			Timezone:  loaded.instance.Listeners.Cron.Timezone,
			Schedules: schedules,
		}, sink)
		if err != nil {
			return nil, fmt.Errorf("configure Cron listener: %w", err)
		}
		components = append(components, component{name: "cron", start: cron.Start})
	}
	return components, nil
}

func resolveReference(environmentName, filename string) (string, error) {
	var value string
	if environmentName != "" {
		var exists bool
		value, exists = os.LookupEnv(environmentName)
		if !exists {
			return "", fmt.Errorf("environment variable %q is not set", environmentName)
		}
	} else {
		content, err := os.ReadFile(filename)
		if err != nil {
			return "", fmt.Errorf("read file %q: %w", filename, err)
		}
		value = string(content)
		if strings.HasSuffix(value, "\r\n") {
			value = strings.TrimSuffix(value, "\r\n")
		} else {
			value = strings.TrimSuffix(value, "\n")
		}
	}
	if strings.IndexByte(value, 0) >= 0 {
		return "", fmt.Errorf("value contains a NUL byte")
	}
	return value, nil
}

func cloneStringMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

type dispatcherSink struct {
	dispatcher *dispatch.Dispatcher
	logger     *slog.Logger
}

func (sink dispatcherSink) Submit(ctx context.Context, document event.Document) error {
	err := sink.dispatcher.Submit(ctx, document)
	if errors.Is(err, dispatch.ErrQueueFull) {
		sink.logger.Warn("event queue rejected submission",
			"event_id", document.EventID,
			"source", document.Source,
			"reason", "queue_full",
		)
		return listener.ErrBackpressure
	}
	return err
}
