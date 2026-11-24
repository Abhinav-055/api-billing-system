package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

var knownDevDefaults = map[string]struct{}{
	"dev-secret": {},
	"dev-salt":   {},
	"password":   {},
}

func Environment() string {
	value := strings.TrimSpace(os.Getenv("ENV"))
	if value == "" {
		return "production"
	}
	return strings.ToLower(value)
}

func RequireSecrets(logger *slog.Logger, required map[string]string) error {
	env := Environment()
	for name, value := range required {
		value = strings.TrimSpace(value)
		_, knownDefault := knownDevDefaults[value]
		if value == "" || (env != "local" && knownDefault) {
			return fmt.Errorf("%s is required in ENV=%s", name, env)
		}
	}
	if env == "local" {
		logger.Warn("running with local development secrets",
			slog.String("ENV", env),
			slog.String("warning", "do not use this environment for production"),
		)
	}
	return nil
}
