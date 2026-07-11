// Package telegram implements Telegram Bot API long-polling listeners.
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/boarkshop/boarkshop/internal/listener"
)

const (
	defaultAPIBase     = "https://api.telegram.org"
	defaultPollTimeout = 30 * time.Second
	defaultRetryDelay  = time.Second
	maxResponseBytes   = 16 << 20
)

// Bot is one configured Telegram bot. ID is a local, non-secret stable name;
// Token is used only for Bot API requests and is never included in an event.
type Bot struct {
	ID          string
	Token       string
	APIBase     string
	PollTimeout time.Duration
}

// Config configures one or more Telegram Bot API long pollers.
type Config struct {
	Bots        []Bot
	APIBase     string
	PollTimeout time.Duration
	RetryDelay  time.Duration
	HTTPClient  *http.Client
}

// Listener polls all configured bots concurrently.
type Listener struct {
	bots       []Bot
	retryDelay time.Duration
	client     *http.Client
	sink       listener.Sink
}

// New validates config without contacting Telegram.
func New(config Config, sink listener.Sink) (*Listener, error) {
	if sink == nil {
		return nil, fmt.Errorf("event sink is required")
	}
	if len(config.Bots) == 0 {
		return nil, fmt.Errorf("at least one Telegram bot is required")
	}
	if config.APIBase == "" {
		config.APIBase = defaultAPIBase
	}
	if config.PollTimeout == 0 {
		config.PollTimeout = defaultPollTimeout
	}
	if config.PollTimeout < 0 {
		return nil, fmt.Errorf("Telegram poll timeout cannot be negative")
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = defaultRetryDelay
	}
	if config.RetryDelay < 0 {
		return nil, fmt.Errorf("Telegram retry delay cannot be negative")
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{}
	}

	bots := append([]Bot(nil), config.Bots...)
	botIDs := make(map[string]struct{}, len(bots))
	botTokens := make(map[string]struct{}, len(bots))
	for i := range bots {
		bots[i].ID = strings.TrimSpace(bots[i].ID)
		if bots[i].ID == "" {
			return nil, fmt.Errorf("Telegram bot ID is required")
		}
		if bots[i].Token == "" {
			return nil, fmt.Errorf("Telegram token for bot %q is required", bots[i].ID)
		}
		if _, exists := botIDs[bots[i].ID]; exists {
			return nil, fmt.Errorf("duplicate Telegram bot ID %q", bots[i].ID)
		}
		if _, exists := botTokens[bots[i].Token]; exists {
			return nil, fmt.Errorf("two Telegram bots cannot use the same token")
		}
		if bots[i].APIBase == "" {
			bots[i].APIBase = config.APIBase
		}
		bots[i].APIBase = strings.TrimRight(bots[i].APIBase, "/")
		parsedBase, err := url.Parse(bots[i].APIBase)
		if err != nil || (parsedBase.Scheme != "http" && parsedBase.Scheme != "https") || parsedBase.Host == "" {
			return nil, fmt.Errorf("Telegram API base for bot %q must be an absolute HTTP(S) URL", bots[i].ID)
		}
		if bots[i].PollTimeout == 0 {
			bots[i].PollTimeout = config.PollTimeout
		}
		if bots[i].PollTimeout < 0 {
			return nil, fmt.Errorf("Telegram poll timeout for bot %q cannot be negative", bots[i].ID)
		}
		botIDs[bots[i].ID] = struct{}{}
		botTokens[bots[i].Token] = struct{}{}
	}

	return &Listener{
		bots:       bots,
		retryDelay: config.RetryDelay,
		client:     config.HTTPClient,
		sink:       sink,
	}, nil
}

// Run long-polls all bots until ctx is canceled. Transient API and
// backpressure failures are retried without advancing the Telegram offset.
func (l *Listener) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var waitGroup sync.WaitGroup
	errorsChannel := make(chan error, len(l.bots))
	for _, bot := range l.bots {
		bot := bot
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			if err := l.pollBot(runCtx, bot); err != nil {
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
		select {
		case err := <-errorsChannel:
			return err
		default:
			return nil
		}
	}
}

// Start is an alias for Run for daemon components with a Start convention.
func (l *Listener) Start(ctx context.Context) error {
	return l.Run(ctx)
}

func (l *Listener) pollBot(ctx context.Context, bot Bot) error {
	var offset int64
	for {
		updates, fatal, err := l.getUpdates(ctx, bot, offset)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if fatal {
				return fmt.Errorf("Telegram bot %q: %w", bot.ID, err)
			}
			if !wait(ctx, l.retryDelay) {
				return nil
			}
			continue
		}

		backpressured := false
		for _, rawUpdate := range updates {
			updateID, fields, err := normalizeUpdate(bot.ID, rawUpdate)
			if err != nil {
				return fmt.Errorf("Telegram bot %q returned an invalid update: %w", bot.ID, err)
			}
			document, err := listener.NewDocument("telegram", time.Now().UTC(), fields)
			if err != nil {
				return fmt.Errorf("create Telegram event for bot %q: %w", bot.ID, err)
			}
			if err := l.sink.Submit(ctx, document); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				backpressured = true
				break
			}
			offset = updateID + 1
		}
		if backpressured && !wait(ctx, l.retryDelay) {
			return nil
		}
	}
}

type getUpdatesResponse struct {
	OK          bool              `json:"ok"`
	Result      []json.RawMessage `json:"result"`
	ErrorCode   int               `json:"error_code"`
	Description string            `json:"description"`
}

func (l *Listener) getUpdates(ctx context.Context, bot Bot, offset int64) ([]json.RawMessage, bool, error) {
	requestCtx, cancel := context.WithTimeout(ctx, time.Duration(pollTimeoutSeconds(bot.PollTimeout))*time.Second+10*time.Second)
	defer cancel()

	form := url.Values{}
	form.Set("offset", strconv.FormatInt(offset, 10))
	form.Set("timeout", strconv.Itoa(pollTimeoutSeconds(bot.PollTimeout)))
	endpoint := bot.APIBase + "/bot" + url.PathEscape(bot.Token) + "/getUpdates"
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, true, fmt.Errorf("construct getUpdates request")
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	response, err := l.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		// Deliberately do not wrap the client error: net/http errors often
		// contain the request URL, and Telegram tokens live in that URL.
		return nil, false, errors.New("getUpdates request failed")
	}
	defer response.Body.Close()

	decoder := json.NewDecoder(io.LimitReader(response.Body, maxResponseBytes))
	var payload getUpdatesResponse
	if err := decoder.Decode(&payload); err != nil {
		return nil, response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden,
			errors.New("getUpdates returned an invalid JSON response")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || !payload.OK {
		fatal := response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden ||
			payload.ErrorCode == http.StatusUnauthorized || payload.ErrorCode == http.StatusForbidden
		description := redact(payload.Description, bot.Token)
		if description == "" {
			description = "Telegram Bot API rejected getUpdates"
		}
		return nil, fatal, errors.New(description)
	}
	return payload.Result, false, nil
}

func normalizeUpdate(botID string, raw json.RawMessage) (int64, map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var update map[string]any
	if err := decoder.Decode(&update); err != nil {
		return 0, nil, fmt.Errorf("decode update: %w", err)
	}
	updateID, ok := integer(update["update_id"])
	if !ok {
		return 0, nil, fmt.Errorf("update_id is missing or is not an integer")
	}
	updateType, payload := findUpdatePayload(update)
	chatID, hasChatID := findChatID(payload)
	userID, hasUserID := findUserID(payload)

	fields := map[string]any{
		"bot_id":      botID,
		"update_id":   updateID,
		"update_type": updateType,
		"chat_id":     nil,
		"user_id":     nil,
		"telegram": map[string]any{
			"update": update,
		},
	}
	if hasChatID {
		fields["chat_id"] = chatID
	}
	if hasUserID {
		fields["user_id"] = userID
	}
	return updateID, fields, nil
}

var knownUpdateTypes = []string{
	"message", "edited_message", "channel_post", "edited_channel_post",
	"business_connection", "business_message", "edited_business_message", "deleted_business_messages",
	"message_reaction", "message_reaction_count", "inline_query", "chosen_inline_result",
	"callback_query", "shipping_query", "pre_checkout_query", "purchased_paid_media",
	"poll", "poll_answer", "my_chat_member", "chat_member", "chat_join_request",
	"chat_boost", "removed_chat_boost",
}

func findUpdatePayload(update map[string]any) (string, map[string]any) {
	for _, updateType := range knownUpdateTypes {
		if value, exists := update[updateType]; exists {
			payload, _ := value.(map[string]any)
			return updateType, payload
		}
	}
	unknown := make([]string, 0, len(update))
	for key := range update {
		if key != "update_id" {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) == 0 {
		return "unknown", nil
	}
	sort.Strings(unknown)
	payload, _ := update[unknown[0]].(map[string]any)
	return unknown[0], payload
}

func findChatID(payload map[string]any) (int64, bool) {
	if id, ok := nestedID(payload, "chat"); ok {
		return id, true
	}
	if message, ok := payload["message"].(map[string]any); ok {
		if id, ok := nestedID(message, "chat"); ok {
			return id, true
		}
	}
	if id, ok := nestedID(payload, "voter_chat"); ok {
		return id, true
	}
	return 0, false
}

func findUserID(payload map[string]any) (int64, bool) {
	if id, ok := nestedID(payload, "from"); ok {
		return id, true
	}
	if id, ok := nestedID(payload, "user"); ok {
		return id, true
	}
	return 0, false
}

func nestedID(object map[string]any, key string) (int64, bool) {
	nested, ok := object[key].(map[string]any)
	if !ok {
		return 0, false
	}
	return integer(nested["id"])
}

func integer(value any) (int64, bool) {
	switch value := value.(type) {
	case json.Number:
		result, err := value.Int64()
		return result, err == nil
	case float64:
		result := int64(value)
		return result, float64(result) == value
	case int64:
		return value, true
	case int:
		return int64(value), true
	default:
		return 0, false
	}
}

func pollTimeoutSeconds(timeout time.Duration) int {
	seconds := int(timeout / time.Second)
	if timeout%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		return 1
	}
	if seconds > 50 {
		return 50
	}
	return seconds
}

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func redact(value, token string) string {
	if token == "" {
		return value
	}
	return strings.ReplaceAll(value, token, "[redacted]")
}
