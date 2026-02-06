package config

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const minimalConfigYAML = `
adapter:
  type: memory

services:
  - name: worker
    identifier: worker
    policies:
      - type: target
        expression: "1"
`

func TestLoadFromEnvOrFile(t *testing.T) {
	t.Run("base64 env takes precedence", func(t *testing.T) {
		t.Setenv(EnvConfigYAML, "adapter: {type: memory}\nservices: []")
		t.Setenv(EnvConfigYAMLBase64, base64.StdEncoding.EncodeToString([]byte(minimalConfigYAML)))

		cfg, err := LoadFromEnvOrFile("does-not-matter.yaml")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Adapter.Type != "memory" {
			t.Fatalf("Adapter.Type = %q, want %q", cfg.Adapter.Type, "memory")
		}
	})

	t.Run("raw env used when base64 unset", func(t *testing.T) {
		t.Setenv(EnvConfigYAML, minimalConfigYAML)
		t.Setenv(EnvConfigYAMLBase64, "")

		cfg, err := LoadFromEnvOrFile("does-not-matter.yaml")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Adapter.Type != "memory" {
			t.Fatalf("Adapter.Type = %q, want %q", cfg.Adapter.Type, "memory")
		}
	})

	t.Run("file used when env unset", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.yaml")
		if err := os.WriteFile(path, []byte(minimalConfigYAML), 0o600); err != nil {
			t.Fatalf("write file: %v", err)
		}

		cfg, err := LoadFromEnvOrFile(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Adapter.Type != "memory" {
			t.Fatalf("Adapter.Type = %q, want %q", cfg.Adapter.Type, "memory")
		}
	})

	t.Run("invalid base64 returns error", func(t *testing.T) {
		t.Setenv(EnvConfigYAMLBase64, "not-base64")

		_, err := LoadFromEnvOrFile("does-not-matter.yaml")
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "decode "+EnvConfigYAMLBase64) {
			t.Fatalf("error = %q, want decode %s", err.Error(), EnvConfigYAMLBase64)
		}
	})
}
