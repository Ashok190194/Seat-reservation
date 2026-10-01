// Package db owns the connection pool and schema migration.
package db

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

func Connect(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = 1
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	// Under a burst, thousands of requests queue for a handful of connections.
	// Fail fast on the handshake only; acquisition waits are bounded by the request context.
	cfg.ConnConfig.ConnectTimeout = 10 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	return pool, nil
}

// Migrate applies the idempotent schema. Safe to run on every boot and from
// several instances at once: it is a single transaction and every statement is IF NOT EXISTS.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	// A session-level advisory lock serialises concurrent boots so two
	// instances never race on CREATE TABLE.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(727001)"); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), "SELECT pg_advisory_unlock(727001)") //nolint:errcheck
	_, err = conn.Exec(ctx, schema)
	return err
}

// Ping is the readiness probe: a real round-trip, not just a pool check.
func Ping(ctx context.Context, pool *pgxpool.Pool) error {
	var one int
	return pool.QueryRow(ctx, "SELECT 1").Scan(&one)
}
