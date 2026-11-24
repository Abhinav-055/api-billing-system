package config

import (
	"log/slog"
	"os"
	"testing"
)

func TestRequireSecretsProduction(t *testing.T) {
	t.Setenv("ENV", "production")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := RequireSecrets(logger, map[string]string{"JWT": "dev-secret"}); err == nil {
		t.Fatal("known dev secret must fail outside local")
	}
}

func TestRequireSecretsLocal(t *testing.T) {
	t.Setenv("ENV", "local")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := RequireSecrets(logger, map[string]string{"JWT": "dev-secret"}); err != nil {
		t.Fatalf("local dev secret should be allowed: %v", err)
	}
}
