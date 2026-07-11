package cron

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/boarkshop/boarkshop/internal/event"
	"github.com/boarkshop/boarkshop/internal/listener"
)

func TestNewAcceptsOnlyStandardFiveFieldExpressions(t *testing.T) {
	sink := listener.SinkFunc(func(context.Context, event.Document) error { return nil })
	if _, err := New(Config{
		Timezone:  "UTC",
		Schedules: []Schedule{{ID: "valid", Expression: "*/5 * * * *"}},
	}, sink); err != nil {
		t.Fatalf("valid standard expression: %v", err)
	}
	if _, err := New(Config{
		Timezone:  "UTC",
		Schedules: []Schedule{{ID: "seconds", Expression: "0 */5 * * * *"}},
	}, sink); err == nil {
		t.Fatal("six-field expression was accepted")
	}
	if _, err := New(Config{
		Timezone:  "UTC",
		Schedules: []Schedule{{ID: "descriptor", Expression: "@every 1m"}},
	}, sink); err == nil {
		t.Fatal("non-five-field descriptor was accepted")
	}
	if _, err := New(Config{Timezone: "Mars/Olympus", Schedules: nil}, sink); err == nil {
		t.Fatal("invalid timezone was accepted")
	}
}

func TestRunEmitsStableEventAndDoesNotCatchUp(t *testing.T) {
	base := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	scheduledAt := base.Add(time.Minute)
	triggeredAt := scheduledAt.Add(2 * time.Second)
	afterTrigger := triggeredAt.Add(10 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	documents := make(chan event.Document, 1)
	cronListener, err := New(Config{
		Timezone:  "UTC",
		Schedules: []Schedule{{ID: "heartbeat", Expression: "* * * * *"}},
	}, listener.SinkFunc(func(_ context.Context, document event.Document) error {
		documents <- document
		cancel()
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	recordingSchedule := &fixedRecordingSchedule{next: scheduledAt}
	cronListener.jobs[0].schedule = recordingSchedule
	clockValues := []time.Time{base, triggeredAt, afterTrigger}
	var clockMu sync.Mutex
	clockIndex := 0
	cronListener.now = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		if clockIndex >= len(clockValues) {
			return clockValues[len(clockValues)-1]
		}
		value := clockValues[clockIndex]
		clockIndex++
		return value
	}
	cronListener.waitUntilDue = func(ctx context.Context, _ time.Time) bool {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}

	runError := make(chan error, 1)
	go func() { runError <- cronListener.Run(ctx) }()

	var document event.Document
	select {
	case document = <-documents:
	case <-time.After(time.Second):
		t.Fatal("cron listener did not emit")
	}
	select {
	case err := <-runError:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cron listener did not stop")
	}

	var root struct {
		Source        string `json:"source"`
		SchemaVersion int    `json:"schema_version"`
		EventID       string `json:"event_id"`
		ReceivedAt    string `json:"received_at"`
		ScheduleID    string `json:"schedule_id"`
		Expression    string `json:"expression"`
		Timezone      string `json:"timezone"`
		ScheduledAt   string `json:"scheduled_at"`
		TriggeredAt   string `json:"triggered_at"`
		Cron          struct {
			ScheduleID string `json:"schedule_id"`
			Expression string `json:"expression"`
		} `json:"cron"`
	}
	if err := json.Unmarshal(document.Root(), &root); err != nil {
		t.Fatal(err)
	}
	if root.Source != "cron" || root.SchemaVersion != listener.SchemaVersion || root.EventID == "" {
		t.Fatalf("invalid common fields: %#v", root)
	}
	if root.ScheduleID != "heartbeat" || root.Expression != "* * * * *" || root.Timezone != "UTC" {
		t.Fatalf("invalid schedule fields: %#v", root)
	}
	if root.ScheduledAt != scheduledAt.Format(time.RFC3339Nano) || root.TriggeredAt != triggeredAt.Format(time.RFC3339Nano) || root.ReceivedAt != root.TriggeredAt {
		t.Fatalf("invalid timestamps: scheduled=%q triggered=%q received=%q", root.ScheduledAt, root.TriggeredAt, root.ReceivedAt)
	}
	if root.Cron.ScheduleID != root.ScheduleID || root.Cron.Expression != root.Expression {
		t.Fatalf("invalid raw cron object: %#v", root.Cron)
	}

	// The second Next call, if the goroutine reached it before observing the
	// cancellation, must be based on the fresh post-trigger wall clock. It must
	// never use the prior scheduled time, which would create catch-up bursts.
	for _, input := range recordingSchedule.inputs() {
		if input.Equal(scheduledAt) {
			t.Fatalf("next occurrence was calculated from the old scheduled time: %s", input)
		}
	}
}

func TestRunWithNoSchedulesWaitsForCancellation(t *testing.T) {
	cronListener, err := New(Config{Timezone: "UTC"}, listener.SinkFunc(func(context.Context, event.Document) error {
		t.Fatal("empty listener submitted an event")
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cronListener.Run(ctx) }()
	select {
	case <-done:
		t.Fatal("empty listener returned before cancellation")
	case <-time.After(10 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("empty listener did not stop")
	}
}

type fixedRecordingSchedule struct {
	mu   sync.Mutex
	next time.Time
	in   []time.Time
}

func (schedule *fixedRecordingSchedule) Next(value time.Time) time.Time {
	schedule.mu.Lock()
	defer schedule.mu.Unlock()
	schedule.in = append(schedule.in, value)
	return schedule.next
}

func (schedule *fixedRecordingSchedule) inputs() []time.Time {
	schedule.mu.Lock()
	defer schedule.mu.Unlock()
	return append([]time.Time(nil), schedule.in...)
}
