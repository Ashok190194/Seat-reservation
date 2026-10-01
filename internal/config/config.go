// Package config reads service configuration from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const DefaultAdminToken = "admin-dev-token"

type Config struct {
	Port          string
	DatabaseURL   string
	AdminToken    string
	TokenSecret   string
	DBMaxConns    int32
	LogLevel      string
	SweepInterval time.Duration
	// IdempotencyTTL is how long a key replays its stored response. After it,
	// the key is purged and a retry becomes a fresh request.
	IdempotencyTTL time.Duration
	// MetricsShowLimit caps how many shows the per-show seat gauges report, to
	// keep /metrics cardinality bounded.
	MetricsShowLimit int
	UsingDefaults    []string
}

func Load() (Config, error) {
	c := Config{
		Port:             getenv("PORT", "8787"),
		DatabaseURL:      os.Getenv("DATABASE_URL"),
		AdminToken:       os.Getenv("ADMIN_TOKEN"),
		TokenSecret:      os.Getenv("TOKEN_SECRET"),
		LogLevel:         getenv("LOG_LEVEL", "info"),
		MetricsShowLimit: 50,
	}
	if c.DatabaseURL == "" {
		return c, fmt.Errorf("DATABASE_URL is required")
	}
	if c.AdminToken == "" {
		c.AdminToken = DefaultAdminToken
		c.UsingDefaults = append(c.UsingDefaults, "ADMIN_TOKEN")
	}
	if c.TokenSecret == "" {
		c.TokenSecret = "dev-token-secret-change-me"
		c.UsingDefaults = append(c.UsingDefaults, "TOKEN_SECRET")
	}
	n, err := strconv.Atoi(getenv("DB_MAX_CONNS", "16"))
	if err != nil || n < 1 {
		return c, fmt.Errorf("DB_MAX_CONNS must be a positive integer")
	}
	c.DBMaxConns = int32(n)
	d, err := time.ParseDuration(getenv("SWEEP_INTERVAL", "1s"))
	if err != nil || d <= 0 {
		return c, fmt.Errorf("SWEEP_INTERVAL must be a positive duration, e.g. 1s")
	}
	c.SweepInterval = d
	ttl, err := time.ParseDuration(getenv("IDEMPOTENCY_TTL", "24h"))
	if err != nil || ttl <= 0 {
		return c, fmt.Errorf("IDEMPOTENCY_TTL must be a positive duration, e.g. 24h")
	}
	c.IdempotencyTTL = ttl
	if v := os.Getenv("METRICS_SHOW_LIMIT"); v != "" {
		if m, err := strconv.Atoi(v); err == nil && m > 0 {
			c.MetricsShowLimit = m
		}
	}
	return c, nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
