package app

import (
	"errors"
	"fmt"
	"os"

	"github.com/boarkshop/boarkshop/internal/config"
	telegramlistener "github.com/boarkshop/boarkshop/internal/listener/telegram"
)

// botCatalogSource loads the dynamic bot directory as a complete generation;
// callers retain their previous generation when discovery, validation, or
// secret resolution fails.
type botCatalogSource struct {
	root         string
	allowMissing bool
}

func (source botCatalogSource) Load() ([]telegramlistener.Bot, error) {
	manifests, err := loadBotManifests(source.root, source.allowMissing)
	if err != nil {
		return nil, err
	}

	bots := make([]telegramlistener.Bot, 0, len(manifests))
	tokens := make(map[string]string, len(manifests))
	appendBot := func(id string, reference config.TokenRef, apiBase string, pollTimeout config.Duration) error {
		token, err := resolveReference(reference.Env, reference.File)
		if err != nil {
			return fmt.Errorf("Telegram bot %q token: %w", id, err)
		}
		if token == "" {
			return fmt.Errorf("Telegram bot %q token is empty", id)
		}
		if previous, exists := tokens[token]; exists {
			return fmt.Errorf("Telegram bots %q and %q cannot use the same token", previous, id)
		}
		tokens[token] = id
		bots = append(bots, telegramlistener.Bot{
			ID:          id,
			Token:       token,
			APIBase:     apiBase,
			PollTimeout: pollTimeout.Std(),
		})
		return nil
	}

	for _, manifest := range manifests {
		if !manifest.Enabled {
			continue
		}
		if err := appendBot(manifest.ID, manifest.Token, manifest.APIBase, manifest.PollTimeout); err != nil {
			return nil, err
		}
	}
	return bots, nil
}

func loadBotManifests(root string, allowMissing bool) ([]*config.TelegramBotManifest, error) {
	info, err := os.Stat(root)
	if errors.Is(err, os.ErrNotExist) && allowMissing {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect Telegram bots directory %q: %w", root, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("Telegram bots path %q is not a directory", root)
	}
	return config.LoadTelegramBots(root)
}
