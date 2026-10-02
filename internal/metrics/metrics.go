// Package metrics exposes Prometheus metrics. Seat gauges are read live from
// the database on every scrape so they always reconcile with GET /shows/{id}.
package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Decline reasons. These are the only values the `reason` label can take.
const (
	ReasonSeatTaken          = "seat_taken"
	ReasonPerUserLimit       = "per_user_limit"
	ReasonIdempotentReplay   = "idempotent_replay"
	ReasonIdempotentMismatch = "idempotency_key_reused"
	ReasonUnknownSeat        = "unknown_seat"
	ReasonShowNotFound       = "show_not_found"
	ReasonInvalidRequest     = "invalid_request"
)

type Metrics struct {
	Registry *prometheus.Registry

	ReservationsConfirmed prometheus.Counter
	ReservationsHeld      prometheus.Counter
	ReservationsDeclined  *prometheus.CounterVec
	ReservationsReplayed  *prometheus.CounterVec
	ReservationsCancelled prometheus.Counter
	ReservationsExpired   prometheus.Counter
	HoldsConfirmed        prometheus.Counter
	IdempotencyKeysPurged prometheus.Counter
	TxRetries             prometheus.Counter

	HTTPRequests *prometheus.CounterVec
	HTTPDuration *prometheus.HistogramVec
	HTTPInFlight prometheus.Gauge

	ReserveDuration prometheus.Histogram
}

func New(pool *pgxpool.Pool, showLimit int, log *slog.Logger) *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		ReservationsConfirmed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "reservations_confirmed_total", Help: "Reservations that ended in status=confirmed (immediate sales plus confirmed holds)."}),
		ReservationsHeld: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "reservations_held_total", Help: "Reservations created as time-boxed holds (shows with hold_ttl_seconds > 0)."}),
		ReservationsDeclined: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reservations_declined_total", Help: "Reserve requests that did not create a new reservation, by reason."}, []string{"reason"}),
		ReservationsReplayed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reservations_replayed_total", Help: "Idempotent replays by the status of the stored original response (a replayed 201 is not a new sale)."}, []string{"original_status"}),
		ReservationsCancelled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "reservations_cancelled_total", Help: "Reservations cancelled by their owner."}),
		ReservationsExpired: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "reservations_expired_total", Help: "Holds released by the expiry sweeper."}),
		HoldsConfirmed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "holds_confirmed_total", Help: "Holds converted to confirmed via POST /reservations/{id}/confirm."}),
		IdempotencyKeysPurged: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "idempotency_keys_purged_total", Help: "Idempotency keys deleted after IDEMPOTENCY_TTL."}),
		TxRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "db_tx_retries_total", Help: "Transactions retried after a serialization failure or deadlock (expected to stay at zero)."}),
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total", Help: "HTTP requests by route and status code."}, []string{"method", "route", "status"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "http_request_duration_seconds", Help: "HTTP request latency.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}}, []string{"method", "route"}),
		HTTPInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "http_requests_in_flight", Help: "Requests currently being served."}),
		ReserveDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "reserve_transaction_duration_seconds", Help: "Wall time of the reserve database transaction, including lock waits.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}}),
	}
	// Pre-register every reason so dashboards see explicit zeros.
	for _, r := range []string{ReasonSeatTaken, ReasonPerUserLimit, ReasonIdempotentReplay, ReasonIdempotentMismatch, ReasonUnknownSeat, ReasonShowNotFound, ReasonInvalidRequest} {
		m.ReservationsDeclined.WithLabelValues(r)
	}
	for _, s := range []string{"201", "409"} {
		m.ReservationsReplayed.WithLabelValues(s)
	}
	reg.MustRegister(
		m.ReservationsConfirmed, m.ReservationsHeld, m.ReservationsDeclined, m.ReservationsReplayed, m.ReservationsCancelled,
		m.ReservationsExpired, m.HoldsConfirmed, m.IdempotencyKeysPurged, m.TxRetries,
		m.HTTPRequests, m.HTTPDuration, m.HTTPInFlight, m.ReserveDuration,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		&seatCollector{pool: pool, limit: showLimit, log: log},
		&poolCollector{pool: pool},
	)
	return m
}

// seatCollector reports per-show seat counts straight from the database.
type seatCollector struct {
	pool  *pgxpool.Pool
	limit int
	log   *slog.Logger
}

var (
	seatsDesc      = prometheus.NewDesc("seats_by_status", "Seats in each status for recently created shows (live from the database).", []string{"show_id", "show_name", "status"}, nil)
	seatsAvailDesc = prometheus.NewDesc("seats_available", "Seats currently available, per show (live from the database).", []string{"show_id", "show_name"}, nil)
	seatsTotalDesc = prometheus.NewDesc("seats_total", "Total seats per show.", []string{"show_id", "show_name"}, nil)
	showsDesc      = prometheus.NewDesc("shows_total", "Number of shows in the system.", nil, nil)
	scrapeErrDesc  = prometheus.NewDesc("seats_scrape_error", "1 if the last database scrape for seat gauges failed.", nil, nil)
	// Database truth: unlike the in-process counters these survive restarts and
	// agree across instances, so they reconcile with the API at any moment.
	reservationsDesc = prometheus.NewDesc("reservations_by_status", "Reservations per show and status (live from the database).", []string{"show_id", "show_name", "status"}, nil)
	mismatchDesc     = prometheus.NewDesc("seat_reservation_mismatches",
		"Taken seats whose reservation disagrees (missing, other status or owner) plus live reservation seats that do not point back. "+
			"Should be 0; it can be briefly non-zero between expiry-sweeper batches.", []string{"show_id", "show_name"}, nil)
)

func (c *seatCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- seatsDesc
	ch <- seatsAvailDesc
	ch <- seatsTotalDesc
	ch <- showsDesc
	ch <- scrapeErrDesc
	ch <- reservationsDesc
	ch <- mismatchDesc
}

func (c *seatCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var shows int64
	if err := c.pool.QueryRow(ctx, "SELECT count(*) FROM shows").Scan(&shows); err != nil {
		c.log.Warn("metrics: show count scrape failed", "err", err)
		ch <- prometheus.MustNewConstMetric(scrapeErrDesc, prometheus.GaugeValue, 1)
		return
	}
	ch <- prometheus.MustNewConstMetric(showsDesc, prometheus.GaugeValue, float64(shows))

	rows, err := c.pool.Query(ctx, `
		SELECT s.id::text, s.name, s.total_seats,
		       count(*) FILTER (WHERE st.status = 'available'),
		       count(*) FILTER (WHERE st.status = 'held'),
		       count(*) FILTER (WHERE st.status = 'confirmed')
		FROM (SELECT id, name, total_seats FROM shows ORDER BY created_at DESC LIMIT $1) s
		JOIN seats st ON st.show_id = s.id
		GROUP BY s.id, s.name, s.total_seats`, c.limit)
	if err != nil {
		c.log.Warn("metrics: seat scrape failed", "err", err)
		ch <- prometheus.MustNewConstMetric(scrapeErrDesc, prometheus.GaugeValue, 1)
		return
	}
	names := map[string]string{}
	for rows.Next() {
		var id, name string
		var total, avail, held, conf int64
		if err := rows.Scan(&id, &name, &total, &avail, &held, &conf); err != nil {
			rows.Close()
			ch <- prometheus.MustNewConstMetric(scrapeErrDesc, prometheus.GaugeValue, 1)
			return
		}
		names[id] = name
		ch <- prometheus.MustNewConstMetric(seatsDesc, prometheus.GaugeValue, float64(avail), id, name, "available")
		ch <- prometheus.MustNewConstMetric(seatsDesc, prometheus.GaugeValue, float64(held), id, name, "held")
		ch <- prometheus.MustNewConstMetric(seatsDesc, prometheus.GaugeValue, float64(conf), id, name, "confirmed")
		ch <- prometheus.MustNewConstMetric(seatsAvailDesc, prometheus.GaugeValue, float64(avail), id, name)
		ch <- prometheus.MustNewConstMetric(seatsTotalDesc, prometheus.GaugeValue, float64(total), id, name)
	}
	rows.Close()
	if rows.Err() != nil || c.collectReservations(ctx, ch, names) != nil {
		ch <- prometheus.MustNewConstMetric(scrapeErrDesc, prometheus.GaugeValue, 1)
		return
	}
	ch <- prometheus.MustNewConstMetric(scrapeErrDesc, prometheus.GaugeValue, 0)
}

// collectReservations emits reservations_by_status and the cross-table mismatch
// count for the shows the seat gauges cover.
func (c *seatCollector) collectReservations(ctx context.Context, ch chan<- prometheus.Metric, names map[string]string) error {
	byStatus := map[string]map[string]int64{}
	rows, err := c.pool.Query(ctx, `
		SELECT r.show_id::text, r.status, count(*)
		FROM reservations r JOIN (SELECT id FROM shows ORDER BY created_at DESC LIMIT $1) s ON s.id = r.show_id
		GROUP BY 1, 2`, c.limit)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id, status string
		var n int64
		if err := rows.Scan(&id, &status, &n); err != nil {
			rows.Close()
			return err
		}
		if byStatus[id] == nil {
			byStatus[id] = map[string]int64{}
		}
		byStatus[id][status] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	mismatches := map[string]int64{}
	rows, err = c.pool.Query(ctx, `
		WITH recent AS (SELECT id FROM shows ORDER BY created_at DESC LIMIT $1)
		SELECT st.show_id::text, count(*) FROM seats st
		LEFT JOIN reservations r ON r.id = st.reservation_id
		WHERE st.show_id IN (SELECT id FROM recent) AND st.status <> 'available'
		  AND (r.id IS NULL OR r.status <> st.status OR r.user_id <> st.user_id OR r.show_id <> st.show_id)
		GROUP BY 1
		UNION ALL
		SELECT r.show_id::text, count(*) FROM reservations r CROSS JOIN LATERAL unnest(r.seats) AS seat
		LEFT JOIN seats st ON st.show_id = r.show_id AND st.label = seat AND st.reservation_id = r.id
		WHERE r.show_id IN (SELECT id FROM recent) AND r.status IN ('held', 'confirmed') AND st.label IS NULL
		GROUP BY 1`, c.limit)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id string
		var n int64
		if err := rows.Scan(&id, &n); err != nil {
			rows.Close()
			return err
		}
		mismatches[id] += n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for id, name := range names {
		for _, status := range []string{"held", "confirmed", "cancelled", "expired"} {
			ch <- prometheus.MustNewConstMetric(reservationsDesc, prometheus.GaugeValue, float64(byStatus[id][status]), id, name, status)
		}
		ch <- prometheus.MustNewConstMetric(mismatchDesc, prometheus.GaugeValue, float64(mismatches[id]), id, name)
	}
	return nil
}

type poolCollector struct{ pool *pgxpool.Pool }

var (
	poolTotalDesc    = prometheus.NewDesc("db_pool_connections_total", "Connections currently open in the pool.", nil, nil)
	poolIdleDesc     = prometheus.NewDesc("db_pool_connections_idle", "Idle connections in the pool.", nil, nil)
	poolMaxDesc      = prometheus.NewDesc("db_pool_connections_max", "Configured pool ceiling.", nil, nil)
	poolWaitDesc     = prometheus.NewDesc("db_pool_acquire_wait_seconds_total", "Cumulative time requests spent waiting for a connection.", nil, nil)
	poolEmptyAcqDesc = prometheus.NewDesc("db_pool_empty_acquires_total", "Acquires that had to wait because the pool was empty.", nil, nil)
)

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- poolTotalDesc
	ch <- poolIdleDesc
	ch <- poolMaxDesc
	ch <- poolWaitDesc
	ch <- poolEmptyAcqDesc
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.pool.Stat()
	ch <- prometheus.MustNewConstMetric(poolTotalDesc, prometheus.GaugeValue, float64(s.TotalConns()))
	ch <- prometheus.MustNewConstMetric(poolIdleDesc, prometheus.GaugeValue, float64(s.IdleConns()))
	ch <- prometheus.MustNewConstMetric(poolMaxDesc, prometheus.GaugeValue, float64(s.MaxConns()))
	ch <- prometheus.MustNewConstMetric(poolWaitDesc, prometheus.CounterValue, s.AcquireDuration().Seconds())
	ch <- prometheus.MustNewConstMetric(poolEmptyAcqDesc, prometheus.CounterValue, float64(s.EmptyAcquireCount()))
}
