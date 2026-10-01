// Command server runs the seat reservation API.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/seatreserve/seatreserve/internal/api"
	"github.com/seatreserve/seatreserve/internal/auth"
	"github.com/seatreserve/seatreserve/internal/config"
	"github.com/seatreserve/seatreserve/internal/db"
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
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)
	for _, d := range cfg.UsingDefaults {
		log.Warn("using insecure default; set the environment variable in production", "var", d)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		log.Error("database connect", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Wait for the database on cold start (managed Postgres often comes up a
	// few seconds after the web process). Readiness stays 503 until this passes.
	if err := waitForDB(ctx, pool, log, 60*time.Second); err != nil {
		log.Error("database never became reachable", "err", err)
		os.Exit(1)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		log.Error("migrate", "err", err)
		os.Exit(1)
	}

	m := metrics.New(pool, cfg.MetricsShowLimit, log)
	st := store.New(pool, log, m.TxRetries.Inc)
	a := auth.New(cfg.TokenSecret, cfg.AdminToken)
	srv := api.New(st, a, m, pool, log)

	go runSweeper(ctx, st, m, log, cfg.SweepInterval)

	httpServer := &http.Server{
		Addr:              net.JoinHostPort("", cfg.Port),
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		// No WriteTimeout on purpose: under a burst, requests legitimately queue
		// for a DB connection and a timeout here would turn declines into errors.
	}
	go func() {
		log.Info("listening", "port", cfg.Port, "db_max_conns", cfg.DBMaxConns, "sweep_interval", cfg.SweepInterval.String())
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Warn("graceful shutdown incomplete", "err", err)
	}
}

func waitForDB(ctx context.Context, pool interface {
	Ping(context.Context) error
}, log *slog.Logger, max time.Duration) error {
	deadline := time.Now().Add(max)
	for attempt := 1; ; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := pool.Ping(pingCtx)
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return err
		}
		log.Warn("database not ready, retrying", "attempt", attempt, "err", err)
		time.Sleep(time.Duration(min(attempt, 5)) * time.Second)
	}
}

// runSweeper releases expired holds. Many instances can run it concurrently;
// SKIP LOCKED makes that safe and the work idempotent.
func runSweeper(ctx context.Context, st *store.Store, m *metrics.Metrics, log *slog.Logger, every time.Duration) {
	const batch = 1000
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
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
