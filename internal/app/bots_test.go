package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/boarkshop/boarkshop/internal/config"
)

func TestBotCatalogSourceReloadsTokenFile(t *testing.T) {
	root := t.TempDir()
	botDir := filepath.Join(root, "bots", "dynamic")
	tokenPath := filepath.Join(botDir, "token")
	writeFile(t, tokenPath, "first-token\n")
	writeFile(t, filepath.Join(botDir, config.TelegramBotFilename), `
version: 1
id: dynamic
token: {file: token}
`)
	source := botCatalogSource{root: filepath.Join(root, "bots")}

	first, err := source.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].Token != "first-token" {
		t.Fatalf("first catalog = %#v", first)
	}
	if err := os.WriteFile(tokenPath, []byte("second-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	second, err := source.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].Token != "second-token" {
		t.Fatalf("second catalog = %#v", second)
	}
}

func TestBotCatalogSourceRejectsDuplicateResolvedToken(t *testing.T) {
	root := t.TempDir()
	for _, id := range []string{"one", "two"} {
		botDir := filepath.Join(root, "bots", id)
		writeFile(t, filepath.Join(botDir, "token"), "same-secret")
		writeFile(t, filepath.Join(botDir, config.TelegramBotFilename), `
version: 1
id: `+id+`
token: {file: token}
`)
	}
	source := botCatalogSource{root: filepath.Join(root, "bots")}

	_, err := source.Load()
	if err == nil || !strings.Contains(err.Error(), "cannot use the same token") {
		t.Fatalf("Load() error = %v", err)
	}
}
