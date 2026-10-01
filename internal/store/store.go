package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound  = errors.New("not found")
	ErrForbidden = errors.New("forbidden")
)

type Store struct {
	pool *pgxpool.Pool
	log  *slog.Logger
	// onRetry is invoked when a transaction is retried after a transient
	// serialization/deadlock error. Wired to a metric by the caller.
	onRetry func()
}

func New(pool *pgxpool.Pool, log *slog.Logger, onRetry func()) *Store {
	if onRetry == nil {
		onRetry = func() {}
	}
	return &Store{pool: pool, log: log, onRetry: onRetry}
}

// ---------------------------------------------------------------------------
// Shows

type CreateShowInput struct {
	Name           string
	Seats          []string
	PricePaise     int64
	PerUserLimit   int
	HoldTTLSeconds int
}

func (s *Store) CreateShow(ctx context.Context, in CreateShowInput) (*ShowState, error) {
	id := uuid.NewString()
	positions := make([]int, len(in.Seats))
	for i := range in.Seats {
		positions[i] = i
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	_, err = tx.Exec(ctx, `INSERT INTO shows (id, name, price_paise, per_user_limit, hold_ttl_seconds, total_seats)
		VALUES ($1, $2, $3, $4, $5, $6)`, id, in.Name, in.PricePaise, in.PerUserLimit, in.HoldTTLSeconds, len(in.Seats))
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO seats (show_id, label, position, status)
		SELECT $1, label, pos, 'available' FROM unnest($2::text[], $3::int[]) AS t(label, pos)`, id, in.Seats, positions)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return s.GetShow(ctx, id)
}

// ShowSummary is a show plus live counts, without the per-seat list.
type ShowSummary struct {
	Show
	Counts SeatCounts `json:"counts"`
}

func (s *Store) ListShows(ctx context.Context, limit int) ([]ShowSummary, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT s.id, s.name, s.price_paise, s.per_user_limit, s.hold_ttl_seconds, s.total_seats, s.created_at,
		       count(*) FILTER (WHERE st.status = 'available'),
		       count(*) FILTER (WHERE st.status = 'held'),
		       count(*) FILTER (WHERE st.status = 'confirmed')
		FROM (SELECT * FROM shows ORDER BY created_at DESC LIMIT $1) s
		JOIN seats st ON st.show_id = s.id
		GROUP BY s.id, s.name, s.price_paise, s.per_user_limit, s.hold_ttl_seconds, s.total_seats, s.created_at
		ORDER BY s.created_at DESC`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ShowSummary{}
	for rows.Next() {
		var sh ShowSummary
		if err := rows.Scan(&sh.ID, &sh.Name, &sh.PricePaise, &sh.PerUserLimit, &sh.HoldTTLSeconds, &sh.TotalSeats, &sh.CreatedAt,
			&sh.Counts.Available, &sh.Counts.Held, &sh.Counts.Confirmed); err != nil {
			return nil, err
		}
		out = append(out, sh)
	}
	return out, rows.Err()
}

func (s *Store) GetShow(ctx context.Context, id string) (*ShowState, error) {
	if _, err := uuid.Parse(id); err != nil {
		return nil, ErrNotFound
	}
	var st ShowState
	err := s.pool.QueryRow(ctx, `SELECT id, name, price_paise, per_user_limit, hold_ttl_seconds, total_seats, created_at
		FROM shows WHERE id = $1`, id).
		Scan(&st.ID, &st.Name, &st.PricePaise, &st.PerUserLimit, &st.HoldTTLSeconds, &st.TotalSeats, &st.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT label, status FROM seats WHERE show_id = $1 ORDER BY position`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	st.Seats = make([]SeatState, 0, st.TotalSeats)
	for rows.Next() {
		var seat SeatState
		if err := rows.Scan(&seat.Label, &seat.Status); err != nil {
			return nil, err
		}
		switch seat.Status {
		case StatusAvailable:
			st.Counts.Available++
		case StatusHeld:
			st.Counts.Held++
		case StatusConfirmed:
			st.Counts.Confirmed++
		}
		st.Seats = append(st.Seats, seat)
	}
	return &st, rows.Err()
}

// ---------------------------------------------------------------------------
// Reserve

type ReserveInput struct {
	ShowID string
	UserID string
	// Seats must already be de-duplicated and sorted by the caller.
	Seats          []string
	IdempotencyKey string // optional
	RequestHash    string // canonical hash of (show, seats); required when a key is present
}

// Reserve is the atomic decision. See WRITEUP.md for the lock-order argument.
//
// Lock order inside the transaction, identical for every caller:
//  1. idempotency_keys primary key (unique-index wait, so same-key duplicates serialise)
//  2. transaction-scoped advisory lock on (show, user) — serialises a single user's parallel requests
//  3. seat rows, SELECT ... ORDER BY label FOR UPDATE — deterministic order, so multi-seat requests cannot deadlock
func (s *Store) Reserve(ctx context.Context, in ReserveInput) (*ReserveOutcome, error) {
	if _, err := uuid.Parse(in.ShowID); err != nil {
		return &ReserveOutcome{Decline: showNotFound()}, nil
	}
	var out *ReserveOutcome
	err := s.withRetry(ctx, func() error {
		var err error
		out, err = s.reserveOnce(ctx, in)
		return err
	})
	return out, err
}

func (s *Store) reserveOnce(ctx context.Context, in ReserveInput) (*ReserveOutcome, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if in.IdempotencyKey != "" {
		tag, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (user_id, key, request_hash, response_code, response_body)
			VALUES ($1, $2, $3, 0, '{}') ON CONFLICT DO NOTHING`, in.UserID, in.IdempotencyKey, in.RequestHash)
		if err != nil {
			return nil, err
		}
		if tag.RowsAffected() == 0 {
			// Someone with this key got here first and has committed (or we
			// would still be blocked on the index). Replay their outcome.
			var hash string
			var code int
			var body string
			err := tx.QueryRow(ctx, `SELECT request_hash, response_code, response_body FROM idempotency_keys
				WHERE user_id = $1 AND key = $2`, in.UserID, in.IdempotencyKey).Scan(&hash, &code, &body)
			if err != nil {
				return nil, err
			}
			if hash != in.RequestHash {
				return &ReserveOutcome{Decline: idempotencyMismatch()}, nil
			}
			if code == 0 {
				// Only reachable if a previous attempt committed a placeholder, which the
				// code never does. Treat as a conflict rather than inventing a result.
				return &ReserveOutcome{Decline: &Decline{HTTPStatus: 409, Code: "idempotency_in_progress",
					Message: "a request with this idempotency key is still being processed"}}, nil
			}
			return &ReserveOutcome{Replayed: true, ReplayStatus: code, ReplayBody: []byte(body)}, nil
		}
	}

	// finish persists the decision: a decline is also recorded against the key,
	// so a retry of a declined request gets the same answer instead of a second try.
	finish := func(out *ReserveOutcome, code int, body []byte) (*ReserveOutcome, error) {
		if in.IdempotencyKey != "" {
			var resID *string
			if out.Reservation != nil {
				resID = &out.Reservation.ID
			}
			if _, err := tx.Exec(ctx, `UPDATE idempotency_keys SET response_code = $3, response_body = $4, reservation_id = $5
				WHERE user_id = $1 AND key = $2`, in.UserID, in.IdempotencyKey, code, string(body), resID); err != nil {
				return nil, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return out, nil
	}
	decline := func(d *Decline) (*ReserveOutcome, error) {
		return finish(&ReserveOutcome{Decline: d}, d.HTTPStatus, d.Body())
	}

	var price int64
	var limit, ttl int
	err = tx.QueryRow(ctx, `SELECT price_paise, per_user_limit, hold_ttl_seconds FROM shows WHERE id = $1`, in.ShowID).
		Scan(&price, &limit, &ttl)
	if errors.Is(err, pgx.ErrNoRows) {
		return decline(showNotFound())
	}
	if err != nil {
		return nil, err
	}
	if len(in.Seats) > limit {
		return decline(perUserLimit(limit, 0, len(in.Seats)))
	}

	// Serialise this user's concurrent requests for this show so the limit
	// check below cannot be raced by the same user on disjoint seats.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1 || ':' || $2, 0))`, in.ShowID, in.UserID); err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx, `SELECT label, status FROM seats WHERE show_id = $1 AND label = ANY($2)
		ORDER BY label FOR UPDATE`, in.ShowID, in.Seats)
	if err != nil {
		return nil, err
	}
	found := make(map[string]string, len(in.Seats))
	for rows.Next() {
		var label, status string
		if err := rows.Scan(&label, &status); err != nil {
			rows.Close()
			return nil, err
		}
		found[label] = status
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var missing, taken []string
	for _, label := range in.Seats {
		status, ok := found[label]
		switch {
		case !ok:
			missing = append(missing, label)
		case status != StatusAvailable:
			taken = append(taken, label)
		}
	}
	if len(missing) > 0 {
		return decline(unknownSeat(missing))
	}
	if len(taken) > 0 {
		// All-or-nothing: one unavailable seat declines the whole request.
		return decline(seatTaken(taken))
	}

	var holding int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM seats WHERE show_id = $1 AND user_id = $2`, in.ShowID, in.UserID).Scan(&holding); err != nil {
		return nil, err
	}
	if holding+len(in.Seats) > limit {
		return decline(perUserLimit(limit, holding, len(in.Seats)))
	}

	res := &Reservation{
		ID:          uuid.NewString(),
		ShowID:      in.ShowID,
		UserID:      in.UserID,
		Seats:       in.Seats,
		AmountPaise: price * int64(len(in.Seats)),
		Status:      StatusConfirmed,
	}
	if ttl > 0 {
		res.Status = StatusHeld
	}
	err = tx.QueryRow(ctx, `INSERT INTO reservations (id, show_id, user_id, seats, amount_paise, status, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, CASE WHEN $7::int > 0 THEN now() + make_interval(secs => $7) END)
		RETURNING expires_at, created_at, updated_at`,
		res.ID, res.ShowID, res.UserID, res.Seats, res.AmountPaise, res.Status, ttl).
		Scan(&res.ExpiresAt, &res.CreatedAt, &res.UpdatedAt)
	if err != nil {
		return nil, err
	}

	// Conditional update guarded on current state: belt and braces on top of the
	// row locks. If this ever updates fewer rows than requested, something
	// bypassed the lock and we refuse rather than double-sell.
	tag, err := tx.Exec(ctx, `UPDATE seats SET status = $3, reservation_id = $4, user_id = $5, hold_expires_at = $6, updated_at = now()
		WHERE show_id = $1 AND label = ANY($2) AND status = 'available'`,
		in.ShowID, in.Seats, res.Status, res.ID, res.UserID, res.ExpiresAt)
	if err != nil {
		return nil, err
	}
	if int(tag.RowsAffected()) != len(in.Seats) {
		s.log.Error("reserve: conditional update touched unexpected row count; refusing",
			"show_id", in.ShowID, "expected", len(in.Seats), "got", tag.RowsAffected())
		return nil, fmt.Errorf("seat state changed under lock (expected %d rows, updated %d)", len(in.Seats), tag.RowsAffected())
	}

	body, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	return finish(&ReserveOutcome{Reservation: res}, 201, body)
}

// ---------------------------------------------------------------------------
// Cancel / Confirm / Get

func (s *Store) GetReservation(ctx context.Context, id, userID string) (*Reservation, error) {
	if _, err := uuid.Parse(id); err != nil {
		return nil, ErrNotFound
	}
	res, err := scanReservation(s.pool.QueryRow(ctx, `SELECT id, show_id, user_id, seats, amount_paise, status, expires_at, created_at, updated_at
		FROM reservations WHERE id = $1`, id))
	if err != nil {
		return nil, err
	}
	if res.UserID != userID {
		return nil, ErrForbidden
	}
	return res, nil
}

// Cancel releases a reservation's seats. Only the owner may cancel. The seat
// release is guarded by reservation_id, so a stale cancel can never free a
// seat that has since been sold to someone else.
func (s *Store) Cancel(ctx context.Context, id, userID string) (*Reservation, error) {
	return s.transition(ctx, id, userID, func(tx pgx.Tx, res *Reservation) (*Reservation, error) {
		switch res.Status {
		case StatusCancelled:
			return res, nil // idempotent
		case StatusExpired:
			return nil, &Decline{HTTPStatus: 409, Code: "reservation_expired", Message: "this hold already expired; nothing to cancel"}
		}
		if _, err := tx.Exec(ctx, `UPDATE seats SET status = 'available', reservation_id = NULL, user_id = NULL, hold_expires_at = NULL, updated_at = now()
			WHERE reservation_id = $1`, id); err != nil {
			return nil, err
		}
		return s.setReservationStatus(ctx, tx, res, StatusCancelled)
	})
}

// Confirm converts a live hold into a confirmed sale.
func (s *Store) Confirm(ctx context.Context, id, userID string) (*Reservation, error) {
	return s.transition(ctx, id, userID, func(tx pgx.Tx, res *Reservation) (*Reservation, error) {
		switch res.Status {
		case StatusConfirmed:
			return res, nil // idempotent
		case StatusCancelled, StatusExpired:
			return nil, &Decline{HTTPStatus: 409, Code: "reservation_" + res.Status,
				Message: "this reservation is " + res.Status + " and cannot be confirmed"}
		}
		tag, err := tx.Exec(ctx, `UPDATE seats SET status = 'confirmed', hold_expires_at = NULL, updated_at = now()
			WHERE reservation_id = $1 AND status = 'held' AND hold_expires_at > now()`, id)
		if err != nil {
			return nil, err
		}
		if int(tag.RowsAffected()) != len(res.Seats) {
			// Hold lapsed between the sweeper's ticks; let the sweeper finish the job.
			return nil, &Decline{HTTPStatus: 409, Code: "hold_expired", Message: "this hold has expired and can no longer be confirmed"}
		}
		return s.setReservationStatus(ctx, tx, res, StatusConfirmed)
	})
}

// transition runs an owner-only state change with the global lock order:
// seat rows first (sorted), then the reservation row.
func (s *Store) transition(ctx context.Context, id, userID string, fn func(pgx.Tx, *Reservation) (*Reservation, error)) (*Reservation, error) {
	if _, err := uuid.Parse(id); err != nil {
		return nil, ErrNotFound
	}
	var result *Reservation
	err := s.withRetry(ctx, func() error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		var owner string
		err = tx.QueryRow(ctx, `SELECT user_id FROM reservations WHERE id = $1`, id).Scan(&owner)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if owner != userID {
			return ErrForbidden
		}
		if _, err := tx.Exec(ctx, `SELECT 1 FROM seats WHERE reservation_id = $1 ORDER BY label FOR UPDATE`, id); err != nil {
			return err
		}
		res, err := scanReservation(tx.QueryRow(ctx, `SELECT id, show_id, user_id, seats, amount_paise, status, expires_at, created_at, updated_at
			FROM reservations WHERE id = $1 FOR UPDATE`, id))
		if err != nil {
			return err
		}
		result, err = fn(tx, res)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	})
	return result, err
}

func (s *Store) setReservationStatus(ctx context.Context, tx pgx.Tx, res *Reservation, status string) (*Reservation, error) {
	err := tx.QueryRow(ctx, `UPDATE reservations SET status = $2, updated_at = now() WHERE id = $1 RETURNING updated_at`,
		res.ID, status).Scan(&res.UpdatedAt)
	if err != nil {
		return nil, err
	}
	res.Status = status
	return res, nil
}

// ---------------------------------------------------------------------------
// Expiry sweeper

// SweepExpired returns expired holds to available. It never waits on a lock
// (SKIP LOCKED), so it can never participate in a deadlock with a reserve.
func (s *Store) SweepExpired(ctx context.Context, batch int) (seats int, reservations int, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	rows, err := tx.Query(ctx, `WITH expired AS (
			SELECT show_id, label, reservation_id FROM seats WHERE status = 'held' AND hold_expires_at <= now()
			ORDER BY show_id, label FOR UPDATE SKIP LOCKED LIMIT $1)
		UPDATE seats s SET status = 'available', reservation_id = NULL, user_id = NULL, hold_expires_at = NULL, updated_at = now()
		FROM expired e WHERE s.show_id = e.show_id AND s.label = e.label
		RETURNING e.reservation_id`, batch)
	if err != nil {
		return 0, 0, err
	}
	ids := map[string]struct{}{}
	for rows.Next() {
		var rid string
		if err := rows.Scan(&rid); err != nil {
			rows.Close()
			return 0, 0, err
		}
		ids[rid] = struct{}{}
		seats++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, 0, err
	}
	if seats == 0 {
		return 0, 0, tx.Rollback(ctx)
	}
	list := make([]string, 0, len(ids))
	for id := range ids {
		list = append(list, id)
	}
	sort.Strings(list)
	tag, err := tx.Exec(ctx, `UPDATE reservations SET status = 'expired', updated_at = now() WHERE id = ANY($1::uuid[]) AND status = 'held'`, list)
	if err != nil {
		return 0, 0, err
	}
	return seats, int(tag.RowsAffected()), tx.Commit(ctx)
}

// ---------------------------------------------------------------------------
// helpers

func scanReservation(row pgx.Row) (*Reservation, error) {
	var r Reservation
	err := row.Scan(&r.ID, &r.ShowID, &r.UserID, &r.Seats, &r.AmountPaise, &r.Status, &r.ExpiresAt, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// withRetry re-runs fn on serialization failures and deadlocks. With the lock
// ordering above these should not occur; the retry is defence in depth so a
// surprise never surfaces as a 5xx.
func (s *Store) withRetry(ctx context.Context, fn func() error) error {
	const attempts = 3
	var err error
	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil || !isTransient(err) || ctx.Err() != nil {
			return err
		}
		s.onRetry()
		s.log.Warn("transaction retry", "attempt", i+1, "err", err)
		time.Sleep(time.Duration(5*(i+1)) * time.Millisecond)
	}
	return err
}

func isTransient(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001", "40P01": // serialization_failure, deadlock_detected
			return true
		}
	}
	return false
}
