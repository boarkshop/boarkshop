package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

const TelegramBotFilename = "bot.yaml"

type telegramBot struct {
	ID          string
	Token       TokenRef
	APIBase     string
	PollTimeout Duration
}

type TokenRef struct {
	Env  string `yaml:"env,omitempty"`
	File string `yaml:"file,omitempty"`
}

// TelegramBotManifest is one independently deployable Telegram bot. Token
// file references are relative to the directory containing bot.yaml.
type TelegramBotManifest struct {
	Version     int      `yaml:"version"`
	ID          string   `yaml:"id"`
	Enabled     bool     `yaml:"enabled"`
	Token       TokenRef `yaml:"token"`
	APIBase     string   `yaml:"api_base"`
	PollTimeout Duration `yaml:"poll_timeout"`
	Dir         string   `yaml:"-"`
	File        string   `yaml:"-"`
}

// LoadTelegramBot loads a bot.yaml file. Passing its containing directory is
// also supported. Token files must be regular files inside that directory.
func LoadTelegramBot(path string) (*TelegramBotManifest, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve Telegram bot path: %w", err)
	}
	if info, statErr := os.Stat(absolute); statErr == nil && info.IsDir() {
		absolute = filepath.Join(absolute, TelegramBotFilename)
	}

	manifest := &TelegramBotManifest{Enabled: true}
	if err := decodeStrictYAML(absolute, manifest); err != nil {
		return nil, fmt.Errorf("load Telegram bot %q: %w", absolute, err)
	}
	manifest.File = filepath.Clean(absolute)
	manifest.Dir = filepath.Dir(manifest.File)
	if manifest.Version != CurrentVersion {
		return nil, fmt.Errorf("validate Telegram bot %q: version must be %d", absolute, CurrentVersion)
	}
	bot := telegramBot{
		ID:          manifest.ID,
		Token:       manifest.Token,
		APIBase:     manifest.APIBase,
		PollTimeout: manifest.PollTimeout,
	}
	if err := normalizeTelegramBot(&bot, func(reference string) (string, error) {
		return resolveSafeReference(manifest.Dir, reference, true)
	}); err != nil {
		return nil, fmt.Errorf("validate Telegram bot %q: %w", absolute, err)
	}
	manifest.ID = bot.ID
	manifest.Token = bot.Token
	manifest.APIBase = bot.APIBase
	manifest.PollTimeout = bot.PollTimeout
	return manifest, nil
}

// LoadTelegramBots discovers bot.yaml in each immediate child directory.
// Directories without bot.yaml and nested manifests are ignored.
func LoadTelegramBots(root string) ([]*TelegramBotManifest, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve Telegram bots directory: %w", err)
	}
	entries, err := os.ReadDir(absoluteRoot)
	if err != nil {
		return nil, fmt.Errorf("read Telegram bots directory %q: %w", absoluteRoot, err)
	}

	result := make([]*TelegramBotManifest, 0, len(entries))
	ids := make(map[string]string)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(absoluteRoot, entry.Name(), TelegramBotFilename)
		if _, err := os.Stat(path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("inspect Telegram bot %q: %w", entry.Name(), err)
		}
		manifest, err := LoadTelegramBot(path)
		if err != nil {
			return nil, err
		}
		if previous, exists := ids[manifest.ID]; exists {
			return nil, fmt.Errorf("duplicate Telegram bot id %q in %q and %q", manifest.ID, previous, manifest.File)
		}
		ids[manifest.ID] = manifest.File
		result = append(result, manifest)
	}
	return result, nil
}

func normalizeTelegramBot(bot *telegramBot, resolveTokenFile func(string) (string, error)) error {
	if !validID(bot.ID) {
		return fmt.Errorf("id %q is invalid", bot.ID)
	}
	if (bot.Token.Env == "") == (bot.Token.File == "") {
		return fmt.Errorf("token must set exactly one of env or file")
	}
	if bot.Token.Env != "" && !validEnvName(bot.Token.Env) {
		return fmt.Errorf("token env %q is invalid", bot.Token.Env)
	}
	if bot.Token.File != "" {
		resolved, err := resolveTokenFile(bot.Token.File)
		if err != nil {
			return fmt.Errorf("token file: %w", err)
		}
		bot.Token.File = resolved
	}
	if bot.APIBase == "" {
		bot.APIBase = DefaultTelegramAPIBase
	}
	parsed, err := url.Parse(bot.APIBase)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("api_base must be an absolute HTTP(S) URL")
	}
	bot.APIBase = strings.TrimRight(bot.APIBase, "/")
	if bot.PollTimeout == 0 {
		bot.PollTimeout = DefaultTelegramPollTimeout
	}
	if bot.PollTimeout <= 0 {
		return fmt.Errorf("poll_timeout must be positive")
	}
	return nil
}
