// Command server runs the seat reservation API.
package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/seatreserve/seatreserve/internal/api"
	"github.com/seatreserve/seatreserve/internal/auth"
	"github.com/seatreserve/seatreserve/internal/config"
	"github.com/seatreserve/seatreserve/internal/db"
	"github.com/seatreserve/seatreserve/internal/logbuf"
	"github.com/seatreserve/seatreserve/internal/metrics"
	"github.com/seatreserve/seatreserve/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("config", "err", err)
		os.Exit(2)
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		level = slog.LevelInfo
	}
	// Every line goes to stdout (the platform's log viewer) and to an in-memory
	// ring that GET /logs serves publicly.
	logRing := logbuf.New(2000)
	log := slog.New(slog.NewJSONHandler(io.MultiWriter(os.Stdout, logRing), &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)
	for _, d := range cfg.UsingDefaults {
		log.Warn("using insecure default; set the environment variable in production", "var", d)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// pgxpool does not dial eagerly, so this only fails on a malformed URL.
	pool, err := db.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		log.Error("database config", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	m := metrics.New(pool, cfg.MetricsShowLimit, log)
	st := store.New(pool, log, m.TxRetries.Inc)
	a := auth.New(cfg.TokenSecret, cfg.AdminToken)
	var ready atomic.Bool
	srv := api.New(st, a, m, pool, log, &ready)
	srv.SetLogBuffer(logRing)

	// Liveness is served immediately; readiness stays 503 until the database is
	// reachable and the schema is applied. Managed Postgres often comes up a few
	// seconds after the web process on a cold start, so keep retrying.
	go func() {
		bootstrapDB(ctx, pool, log)
		if ctx.Err() != nil {
			return
		}
		ready.Store(true)
		log.Info("ready: database reachable and schema applied")
		runSweeper(ctx, st, m, log, cfg.SweepInterval, cfg.IdempotencyTTL)
	}()

	httpServer := &http.Server{
		Addr:              net.JoinHostPort("", cfg.Port),
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		// No WriteTimeout on purpose: under a burst, requests legitimately queue
		// for a DB connection and a timeout here would turn declines into errors.
	}
	go func() {
		log.Info("listening", "port", cfg.Port, "db_max_conns", cfg.DBMaxConns, "sweep_interval", cfg.SweepInterval.String(), "idempotency_ttl", cfg.IdempotencyTTL.String())
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	// Fail readiness first so the platform stops sending new requests, then drain.
	srv.Drain()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Warn("graceful shutdown incomplete", "err", err)
	}
}

// bootstrapDB pings and migrates until it succeeds or the process is told to stop.
func bootstrapDB(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) {
	for attempt := 1; ctx.Err() == nil; attempt++ {
		stepCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := pool.Ping(stepCtx)
		if err == nil {
			err = db.Migrate(stepCtx, pool)
		}
		cancel()
		if err == nil {
			return
		}
		log.Warn("database not ready, retrying", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(min(attempt, 5)) * time.Second):
		}
	}
}

// runSweeper releases expired holds. Many instances can run it concurrently;
// SKIP LOCKED makes that safe and the work idempotent.
func runSweeper(ctx context.Context, st *store.Store, m *metrics.Metrics, log *slog.Logger, every, idemTTL time.Duration) {
	const batch = 1000
	t := time.NewTicker(every)
	defer t.Stop()
	// Key purging is housekeeping, not latency-sensitive: once a minute is plenty.
	purge := time.NewTicker(time.Minute)
	defer purge.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-purge.C:
			purgeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			n, err := st.PurgeIdempotencyKeys(purgeCtx, idemTTL, batch)
			cancel()
			if err != nil && ctx.Err() == nil {
				log.Warn("idempotency key purge failed", "err", err)
			} else if n > 0 {
				m.IdempotencyKeysPurged.Add(float64(n))
				log.Info("idempotency keys purged", "count", n, "older_than", idemTTL.String())
			}
			continue
		case <-t.C:
		}
		for {
			sweepCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			seats, reservations, err := st.SweepExpired(sweepCtx, batch)
			cancel()
			if err != nil {
				if ctx.Err() == nil {
					log.Warn("sweep failed", "err", err)
				}
				break
			}
			if reservations > 0 {
				m.ReservationsExpired.Add(float64(reservations))
				log.Info("expired holds released", "seats", seats, "reservations", reservations)
			}
			if seats < batch {
				break
			}
		}
	}
}
