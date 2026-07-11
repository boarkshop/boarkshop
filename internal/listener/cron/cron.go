// Package cron implements scheduled event emission.
package cron

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	robfigcron "github.com/robfig/cron/v3"

	"github.com/boarkshop/boarkshop/internal/listener"
)

// Schedule is one named standard five-field cron expression. Seconds are not
// accepted; this matches robfig/cron ParseStandard and the instance config.
type Schedule struct {
	ID         string
	Expression string
}

// Config configures all cron schedules for an instance.
type Config struct {
	Timezone  string
	Schedules []Schedule
}

type scheduledJob struct {
	config   Schedule
	schedule robfigcron.Schedule
}

// Listener emits a single event for each due occurrence.
type Listener struct {
	timezone     string
	location     *time.Location
	jobs         []scheduledJob
	sink         listener.Sink
	now          func() time.Time
	waitUntilDue func(context.Context, time.Time) bool
}

// New parses all expressions eagerly so invalid schedules cannot fail after
// the daemon has started.
func New(config Config, sink listener.Sink) (*Listener, error) {
	if sink == nil {
		return nil, fmt.Errorf("event sink is required")
	}
	if config.Timezone == "" {
		config.Timezone = "UTC"
	}
	location, err := time.LoadLocation(config.Timezone)
	if err != nil {
		return nil, fmt.Errorf("load cron timezone %q: %w", config.Timezone, err)
	}

	jobs := make([]scheduledJob, 0, len(config.Schedules))
	ids := make(map[string]struct{}, len(config.Schedules))
	for index, item := range config.Schedules {
		item.ID = strings.TrimSpace(item.ID)
		if item.ID == "" {
			return nil, fmt.Errorf("cron schedule %d has an empty ID", index)
		}
		if _, exists := ids[item.ID]; exists {
			return nil, fmt.Errorf("duplicate cron schedule ID %q", item.ID)
		}
		if len(strings.Fields(item.Expression)) != 5 {
			return nil, fmt.Errorf("cron schedule %q must contain exactly five fields", item.ID)
		}
		parsed, err := robfigcron.ParseStandard(item.Expression)
		if err != nil {
			return nil, fmt.Errorf("parse cron schedule %q: %w", item.ID, err)
		}
		ids[item.ID] = struct{}{}
		jobs = append(jobs, scheduledJob{config: item, schedule: parsed})
	}

	l := &Listener{
		timezone: config.Timezone,
		location: location,
		jobs:     jobs,
		sink:     sink,
		now:      time.Now,
	}
	l.waitUntilDue = waitUntilDue
	return l, nil
}

// Run emits events until ctx is canceled. Every next occurrence is computed
// from the current wall clock after the prior occurrence; missed occurrences
// are therefore never replayed (no catch-up).
func (l *Listener) Run(ctx context.Context) error {
	if len(l.jobs) == 0 {
		<-ctx.Done()
		return nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var waitGroup sync.WaitGroup
	errorsChannel := make(chan error, len(l.jobs))
	for _, job := range l.jobs {
		job := job
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			if err := l.runJob(runCtx, job); err != nil {
				select {
				case errorsChannel <- err:
				default:
				}
				cancel()
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		waitGroup.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		cancel()
		<-done
		return nil
	case err := <-errorsChannel:
		cancel()
		<-done
		return err
	case <-done:
		return nil
	}
}

// Start is an alias for Run for daemon components with a Start convention.
func (l *Listener) Start(ctx context.Context) error {
	return l.Run(ctx)
}

func (l *Listener) runJob(ctx context.Context, job scheduledJob) error {
	for {
		now := l.now().In(l.location)
		scheduledAt := job.schedule.Next(now)
		if scheduledAt.IsZero() {
			return fmt.Errorf("cron schedule %q has no future occurrence", job.config.ID)
		}
		if !l.waitUntilDue(ctx, scheduledAt) {
			return nil
		}

		triggeredAt := l.now().UTC()
		if err := l.emit(ctx, job.config, scheduledAt, triggeredAt); err != nil {
			return err
		}
		// Intentionally calculate the next occurrence from a fresh current time
		// on the next loop iteration. This prevents catch-up bursts.
	}
}

func (l *Listener) emit(ctx context.Context, schedule Schedule, scheduledAt, triggeredAt time.Time) error {
	document, err := listener.NewDocument("cron", triggeredAt.UTC(), map[string]any{
		"schedule_id":  schedule.ID,
		"expression":   schedule.Expression,
		"timezone":     l.timezone,
		"scheduled_at": scheduledAt.UTC().Format(time.RFC3339Nano),
		"triggered_at": triggeredAt.UTC().Format(time.RFC3339Nano),
		"cron": map[string]any{
			"schedule_id": schedule.ID,
			"expression":  schedule.Expression,
		},
	})
	if err != nil {
		return fmt.Errorf("create cron event for schedule %q: %w", schedule.ID, err)
	}
	// Backpressure drops this occurrence. Retrying it later would violate the
	// explicit no-catch-up contract.
	_ = l.sink.Submit(ctx, document)
	return nil
}

func waitUntilDue(ctx context.Context, scheduledAt time.Time) bool {
	delay := time.Until(scheduledAt)
	if delay < 0 {
		delay = 0
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
