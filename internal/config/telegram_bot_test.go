package config

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadTelegramBotsFromImmediateDirectories(t *testing.T) {
	root := t.TempDir()
	botDir := filepath.Join(root, "support")
	writeTestFile(t, filepath.Join(botDir, "token"), "secret-token\n")
	writeTestFile(t, filepath.Join(botDir, TelegramBotFilename), `
version: 1
id: support
token:
  file: token
poll_timeout: 2s
`)
	writeTestFile(t, filepath.Join(root, "container", "nested", TelegramBotFilename), `
version: 1
id: nested
token: {env: NESTED_TOKEN}
`)

	bots, err := LoadTelegramBots(root)
	if err != nil {
		t.Fatalf("LoadTelegramBots() error = %v", err)
	}
	if len(bots) != 1 {
		t.Fatalf("len(bots) = %d, want 1", len(bots))
	}
	bot := bots[0]
	if bot.ID != "support" || !bot.Enabled {
		t.Fatalf("bot identity = %q, enabled=%v", bot.ID, bot.Enabled)
	}
	if bot.Token.File != filepath.Join(botDir, "token") {
		t.Errorf("token file = %q", bot.Token.File)
	}
	if bot.APIBase != DefaultTelegramAPIBase {
		t.Errorf("api base = %q", bot.APIBase)
	}
	if bot.PollTimeout.Std() != 2*time.Second {
		t.Errorf("poll timeout = %s", bot.PollTimeout)
	}
}

func TestLoadTelegramBotRejectsUnsafeTokenFile(t *testing.T) {
	root := t.TempDir()
	botDir := filepath.Join(root, "bot")
	writeTestFile(t, filepath.Join(root, "outside-token"), "secret")
	writeTestFile(t, filepath.Join(botDir, TelegramBotFilename), `
version: 1
id: unsafe
token:
  file: ../outside-token
`)

	_, err := LoadTelegramBot(botDir)
	if err == nil || (!strings.Contains(err.Error(), "must be relative") && !strings.Contains(err.Error(), "escapes")) {
		t.Fatalf("LoadTelegramBot() error = %v, want unsafe-reference error", err)
	}
}

func TestLoadTelegramBotsRejectsDuplicateIDs(t *testing.T) {
	root := t.TempDir()
	for _, directory := range []string{"one", "two"} {
		writeTestFile(t, filepath.Join(root, directory, TelegramBotFilename), `
version: 1
id: duplicate
token:
  env: TOKEN_`+strings.ToUpper(directory)+`
`)
	}

	_, err := LoadTelegramBots(root)
	if err == nil || !strings.Contains(err.Error(), `duplicate Telegram bot id "duplicate"`) {
		t.Fatalf("LoadTelegramBots() error = %v", err)
	}
}
