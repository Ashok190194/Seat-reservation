// Package api is the HTTP layer: routing, auth, validation, response shaping.
// It contains no business decisions; those live in package store.
package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/seatreserve/seatreserve/internal/auth"
	"github.com/seatreserve/seatreserve/internal/db"
	"github.com/seatreserve/seatreserve/internal/logbuf"
	"github.com/seatreserve/seatreserve/internal/metrics"
	"github.com/seatreserve/seatreserve/internal/store"
)

//go:embed dashboard.html
var dashboardHTML []byte

const (
	maxBodyBytes      = 64 << 10
	maxShowBodyBytes  = 2 << 20 // 20,000 labels of 32 bytes is ~700 KB of JSON
	maxSeatsPerShow   = 20000
	maxSeatLabelLen   = 32
	maxIdemKeyLen     = 128
	defaultUserLimit  = 4
	maxPerUserLimit   = 100
	maxHoldTTLSeconds = 86400
	// ₹10 crore per seat. With at most 100 seats per request the amount stays
	// far below the int64 limit, so price × seats can never wrap.
	maxPricePaise = 10_000_000_000
)

type Server struct {
	store   *store.Store
	auth    *auth.Authenticator
	metrics *metrics.Metrics
	pool    *pgxpool.Pool
	log     *slog.Logger
	// ready flips to true once the schema has been applied; until then
	// /readyz reports 503 even if the database answers pings.
	ready *atomic.Bool
	// logs holds this instance's recent log lines for GET /logs (nil disables it).
	logs *logbuf.Ring
	// draining is set on SIGTERM so /readyz fails while in-flight requests finish.
	draining atomic.Bool
}

// Drain makes /readyz report 503 so the platform stops routing new requests
// here while the server finishes the ones it has.
func (s *Server) Drain() { s.draining.Store(true) }

func New(st *store.Store, a *auth.Authenticator, m *metrics.Metrics, pool *pgxpool.Pool, log *slog.Logger, ready *atomic.Bool) *Server {
	return &Server{store: st, auth: a, metrics: m, pool: pool, log: log, ready: ready}
}

// SetLogBuffer enables GET /logs, served from the ring the logger also writes to.
func (s *Server) SetLogBuffer(r *logbuf.Ring) { s.logs = r }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.dashboard)
	mux.HandleFunc("GET /favicon.ico", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /logs", s.recentLogs)

	mux.HandleFunc("POST /auth/token", s.issueToken)
	mux.HandleFunc("POST /shows", s.createShow)
	mux.HandleFunc("GET /shows", s.listShows)
	mux.HandleFunc("GET /shows/{id}", s.getShow)
	mux.HandleFunc("POST /shows/{id}/reserve", s.reserve)
	mux.HandleFunc("GET /users/me/reservations", s.myReservations)
	mux.HandleFunc("GET /reservations/{id}", s.getReservation)
	mux.HandleFunc("POST /reservations/{id}/cancel", s.cancel)
	mux.HandleFunc("POST /reservations/{id}/confirm", s.confirm)

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "no such route")
	})
	return s.observe(mux)
}

// ---------------------------------------------------------------------------
// Health

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz fails closed: if the database cannot answer SELECT 1 within 2s we
// report 503 so the platform stops routing traffic to this instance.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "draining", "checks": map[string]any{"database": map[string]any{"ok": false, "error": "shutting down"}},
		})
		return
	}
	if !s.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "starting", "checks": map[string]any{"database": map[string]any{"ok": false, "error": "schema not yet applied"}},
		})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := db.Ping(ctx, s.pool); err != nil {
		// The detail (host, user, database name) goes to the log, not to anonymous callers.
		s.log.Warn("readiness check failed", "request_id", RequestID(r), "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "unavailable", "checks": map[string]any{"database": map[string]any{"ok": false, "error": "database unreachable"}},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ready", "checks": map[string]any{"database": map[string]any{"ok": true, "latency_ms": float64(time.Since(start).Microseconds()) / 1000}},
	})
}

// recentLogs serves this instance's most recent JSON log lines, oldest first.
// Filters: ?request_id= (exact), ?q= (substring). ?after=<next from the previous
// response> returns only lines written since, so a client can tail the log.
// The buffer is per instance and starts empty on every restart.
func (s *Server) recentLogs(w http.ResponseWriter, r *http.Request) {
	if s.logs == nil {
		writeError(w, http.StatusNotFound, "not_found", "the log buffer is not enabled")
		return
	}
	q := r.URL.Query()
	limit := 200
	if v, err := strconv.Atoi(q.Get("limit")); err == nil && v > 0 {
		limit = min(v, 2000)
	}
	after, _ := strconv.ParseUint(q.Get("after"), 10, 64)
	var needles [][]byte
	if id := strings.TrimSpace(q.Get("request_id")); id != "" {
		needles = append(needles, []byte(`"request_id":"`+id+`"`))
	}
	if text := q.Get("q"); text != "" {
		needles = append(needles, []byte(text))
	}
	lines, next := s.logs.Since(after, limit, func(line []byte) bool {
		for _, n := range needles {
			if !bytes.Contains(line, n) {
				return false
			}
		}
		return true
	})
	raw := make([]json.RawMessage, len(lines))
	for i, l := range lines {
		raw[i] = l
	}
	writeJSON(w, http.StatusOK, map[string]any{"next": next, "lines": raw})
}

func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Write(dashboardHTML) //nolint:errcheck
}

// ---------------------------------------------------------------------------
// Auth

func (s *Server) issueToken(w http.ResponseWriter, r *http.Request) {
	var body struct {
		UserID string `json:"user_id"`
	}
	if !decodeBody(w, r, &body, maxBodyBytes) {
		return
	}
	userID := strings.TrimSpace(body.UserID)
	tok, err := s.auth.Issue(userID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_user_id", err.Error())
		return
	}
	annotate(r, userID, "token_issued")
	writeJSON(w, http.StatusCreated, map[string]string{"user_id": userID, "token": tok, "token_type": "Bearer"})
}

// requireUser returns the token's user id or writes a 401.
func (s *Server) requireUser(w http.ResponseWriter, r *http.Request) (string, bool) {
	tok, ok := auth.BearerToken(r.Header.Get("Authorization"))
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized", "missing bearer token")
		return "", false
	}
	user, err := s.auth.Verify(tok)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized", "invalid bearer token")
		return "", false
	}
	annotate(r, user, "")
	return user, true
}

func (s *Server) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	tok, ok := auth.BearerToken(r.Header.Get("Authorization"))
	if !ok || !s.auth.IsAdmin(tok) {
		writeError(w, http.StatusUnauthorized, "unauthorized", "admin token required")
		return false
	}
	annotate(r, "admin", "")
	return true
}

// ---------------------------------------------------------------------------
// Shows

func (s *Server) createShow(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	var body struct {
		Name           string   `json:"name"`
		Seats          []string `json:"seats"`
		PricePaise     *int64   `json:"price_paise"`
		PerUserLimit   *int     `json:"per_user_limit"`
		HoldTTLSeconds *int     `json:"hold_ttl_seconds"`
	}
	if !decodeBody(w, r, &body, maxShowBodyBytes) {
		return
	}
	body.Name = strings.TrimSpace(body.Name)
	switch {
	case body.Name == "" || len(body.Name) > 200 || !printableText(body.Name):
		writeError(w, http.StatusBadRequest, "invalid_request", "name is required (1-200 bytes of printable text)")
		return
	case body.PricePaise == nil || *body.PricePaise < 0 || *body.PricePaise > maxPricePaise:
		writeError(w, http.StatusBadRequest, "invalid_request", "price_paise is required and must be an integer from 0 to 10000000000")
		return
	case len(body.Seats) == 0 || len(body.Seats) > maxSeatsPerShow:
		writeError(w, http.StatusBadRequest, "invalid_request", "seats must contain between 1 and 20000 labels")
		return
	}
	seen := make(map[string]struct{}, len(body.Seats))
	seats := make([]string, 0, len(body.Seats))
	for _, raw := range body.Seats {
		label := strings.TrimSpace(raw)
		if label == "" || len(label) > maxSeatLabelLen || !printableText(label) {
			writeError(w, http.StatusBadRequest, "invalid_request", "seat labels must be 1-32 bytes of printable text")
			return
		}
		if _, dup := seen[label]; dup {
			writeError(w, http.StatusBadRequest, "invalid_request", "duplicate seat label: "+label)
			return
		}
		seen[label] = struct{}{}
		seats = append(seats, label)
	}
	in := store.CreateShowInput{Name: body.Name, Seats: seats, PricePaise: *body.PricePaise, PerUserLimit: defaultUserLimit}
	if body.PerUserLimit != nil {
		if *body.PerUserLimit < 1 || *body.PerUserLimit > maxPerUserLimit {
			writeError(w, http.StatusBadRequest, "invalid_request", "per_user_limit must be between 1 and 100")
			return
		}
		in.PerUserLimit = *body.PerUserLimit
	}
	if body.HoldTTLSeconds != nil {
		if *body.HoldTTLSeconds < 0 || *body.HoldTTLSeconds > maxHoldTTLSeconds {
			writeError(w, http.StatusBadRequest, "invalid_request", "hold_ttl_seconds must be between 0 and 86400")
			return
		}
		in.HoldTTLSeconds = *body.HoldTTLSeconds
	}
	st, err := s.store.CreateShow(r.Context(), in)
	if err != nil {
		s.serverError(w, r, "create show", err)
		return
	}
	annotate(r, "", "show_created")
	s.log.Info("show created", "request_id", RequestID(r), "show_id", st.ID, "name", st.Name, "seats", st.TotalSeats,
		"price_paise", st.PricePaise, "per_user_limit", st.PerUserLimit, "hold_ttl_seconds", st.HoldTTLSeconds)
	writeJSON(w, http.StatusCreated, st)
}

func (s *Server) listShows(w http.ResponseWriter, r *http.Request) {
	shows, err := s.store.ListShows(r.Context(), 20)
	if err != nil {
		s.serverError(w, r, "list shows", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"shows": shows})
}

func (s *Server) getShow(w http.ResponseWriter, r *http.Request) {
	id, ok := canonicalID(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "show_not_found", "show does not exist")
		return
	}
	st, err := s.store.GetShow(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "show_not_found", "show does not exist")
		return
	}
	if err != nil {
		s.serverError(w, r, "get show", err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

// ---------------------------------------------------------------------------
// Reserve

func (s *Server) reserve(w http.ResponseWriter, r *http.Request) {
	user, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	var body struct {
		Seats          []string `json:"seats"`
		IdempotencyKey *string  `json:"idempotency_key"`
		// user_id is deliberately not read: identity is the token's, full stop.
	}
	if !decodeBody(w, r, &body, maxBodyBytes) {
		s.metrics.ReservationsDeclined.WithLabelValues(metrics.ReasonInvalidRequest).Inc()
		return
	}
	headerKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	bodyKey := ""
	if body.IdempotencyKey != nil {
		bodyKey = strings.TrimSpace(*body.IdempotencyKey)
	}
	// A key that is sent but blank would otherwise silently turn idempotency off.
	if (len(r.Header.Values("Idempotency-Key")) > 0 && headerKey == "") || (body.IdempotencyKey != nil && bodyKey == "") {
		s.declineInvalid(w, r, "idempotency key is blank; send a non-empty key or leave it out")
		return
	}
	key := headerKey
	if key == "" {
		key = bodyKey
	}
	if headerKey != "" && bodyKey != "" && headerKey != bodyKey {
		s.declineInvalid(w, r, "Idempotency-Key header and idempotency_key body field disagree")
		return
	}
	if len(key) > maxIdemKeyLen || !printableASCII(key) {
		s.declineInvalid(w, r, "idempotency key must be at most 128 printable ASCII characters")
		return
	}
	if len(body.Seats) == 0 {
		s.declineInvalid(w, r, "seats must contain at least one seat label")
		return
	}
	if len(body.Seats) > maxPerUserLimit {
		s.declineInvalid(w, r, "too many seats in one request")
		return
	}
	seatSet := make(map[string]struct{}, len(body.Seats))
	for _, raw := range body.Seats {
		label := strings.TrimSpace(raw)
		if label == "" || len(label) > maxSeatLabelLen || !printableText(label) {
			s.declineInvalid(w, r, "seat labels must be 1-32 bytes of printable text")
			return
		}
		seatSet[label] = struct{}{}
	}
	seats := make([]string, 0, len(seatSet))
	for label := range seatSet {
		seats = append(seats, label)
	}
	sort.Strings(seats)

	// One spelling per show: the advisory lock key, the request hash and the
	// response all use the canonical id, so /shows/ABC… and /shows/abc… are
	// the same show for the per-user limit and for idempotency.
	showID, ok := canonicalID(r.PathValue("id"))
	if !ok {
		s.metrics.ReservationsDeclined.WithLabelValues(metrics.ReasonShowNotFound).Inc()
		annotate(r, "", metrics.ReasonShowNotFound)
		writeError(w, http.StatusNotFound, "show_not_found", "show does not exist")
		return
	}
	in := store.ReserveInput{ShowID: showID, UserID: user, Seats: seats, IdempotencyKey: key}
	if key != "" {
		in.RequestHash = requestHash(showID, seats)
	}

	start := time.Now()
	out, err := s.store.Reserve(r.Context(), in)
	s.metrics.ReserveDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		s.serverError(w, r, "reserve", err)
		return
	}

	switch {
	case out.Replayed:
		s.metrics.ReservationsDeclined.WithLabelValues(metrics.ReasonIdempotentReplay).Inc()
		s.metrics.ReservationsReplayed.WithLabelValues(strconv.Itoa(out.ReplayStatus)).Inc()
		annotate(r, "", "idempotent_replay")
		w.Header().Set("Idempotent-Replayed", "true")
		writeRaw(w, out.ReplayStatus, out.ReplayBody)
	case out.Decline != nil:
		s.metrics.ReservationsDeclined.WithLabelValues(out.Decline.Code).Inc()
		annotate(r, "", out.Decline.Code)
		writeRaw(w, out.Decline.HTTPStatus, out.Decline.Body())
	default:
		res := out.Reservation
		if res.Status == store.StatusHeld {
			s.metrics.ReservationsHeld.Inc()
		} else {
			s.metrics.ReservationsConfirmed.Inc()
		}
		annotate(r, "", "reserved_"+res.Status)
		s.log.Info("reservation created", "request_id", RequestID(r), "reservation_id", res.ID, "show_id", res.ShowID,
			"user_id", res.UserID, "seats", res.Seats, "amount_paise", res.AmountPaise, "status", res.Status)
		writeJSON(w, http.StatusCreated, res)
	}
}

func (s *Server) declineInvalid(w http.ResponseWriter, r *http.Request, msg string) {
	s.metrics.ReservationsDeclined.WithLabelValues(metrics.ReasonInvalidRequest).Inc()
	annotate(r, "", "invalid_request")
	writeError(w, http.StatusBadRequest, "invalid_request", msg)
}

// requestHash canonicalises what the idempotency key must stay bound to.
func requestHash(showID string, sortedSeats []string) string {
	h := sha256.New()
	h.Write([]byte(showID))
	for _, s := range sortedSeats {
		h.Write([]byte{0})
		h.Write([]byte(s))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ---------------------------------------------------------------------------
// Reservations

// myReservations lists the token holder's reservations. Filter with ?show_id=.
func (s *Server) myReservations(w http.ResponseWriter, r *http.Request) {
	user, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	showID := strings.TrimSpace(r.URL.Query().Get("show_id"))
	if showID != "" {
		id, ok := canonicalID(showID)
		if !ok {
			writeJSON(w, http.StatusOK, map[string]any{"user_id": user, "reservations": []store.Reservation{}})
			return
		}
		showID = id
	}
	list, err := s.store.ListUserReservations(r.Context(), user, showID, 100)
	if err != nil {
		s.serverError(w, r, "list reservations", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user_id": user, "reservations": list})
}

// reservationID returns the canonical reservation id from the path, or writes a 404.
func reservationID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id, ok := canonicalID(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "reservation_not_found", "reservation does not exist")
	}
	return id, ok
}

func (s *Server) getReservation(w http.ResponseWriter, r *http.Request) {
	user, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	id, ok := reservationID(w, r)
	if !ok {
		return
	}
	res, err := s.store.GetReservation(r.Context(), id, user)
	s.writeReservationResult(w, r, res, err)
}

// cancel and confirm are idempotent: repeating one returns 200 with the
// current state. Counters and lifecycle logs move only when the state
// actually changed, so they keep reconciling with the database.
func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	user, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	id, ok := reservationID(w, r)
	if !ok {
		return
	}
	res, changed, err := s.store.Cancel(r.Context(), id, user)
	if s.writeReservationResult(w, r, res, err) && changed {
		s.metrics.ReservationsCancelled.Inc()
		annotate(r, "", "cancelled")
		s.log.Info("reservation cancelled", "request_id", RequestID(r), "reservation_id", res.ID, "show_id", res.ShowID, "user_id", user, "seats", res.Seats)
	}
}

func (s *Server) confirm(w http.ResponseWriter, r *http.Request) {
	user, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	id, ok := reservationID(w, r)
	if !ok {
		return
	}
	res, changed, err := s.store.Confirm(r.Context(), id, user)
	if s.writeReservationResult(w, r, res, err) && changed {
		s.metrics.HoldsConfirmed.Inc()
		s.metrics.ReservationsConfirmed.Inc()
		annotate(r, "", "hold_confirmed")
		s.log.Info("hold confirmed", "request_id", RequestID(r), "reservation_id", res.ID, "show_id", res.ShowID, "user_id", user, "seats", res.Seats)
	}
}

// writeReservationResult maps store results to HTTP. Returns true when a
// reservation was written with 200.
func (s *Server) writeReservationResult(w http.ResponseWriter, r *http.Request, res *store.Reservation, err error) bool {
	var d *store.Decline
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, res)
		return true
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "reservation_not_found", "reservation does not exist")
	case errors.Is(err, store.ErrForbidden):
		annotate(r, "", "forbidden")
		writeError(w, http.StatusForbidden, "forbidden", "this reservation belongs to another user")
	case errors.As(err, &d):
		annotate(r, "", d.Code)
		writeRaw(w, d.HTTPStatus, d.Body())
	default:
		s.serverError(w, r, "reservation transition", err)
	}
	return false
}

// ---------------------------------------------------------------------------
// Plumbing

func (s *Server) serverError(w http.ResponseWriter, r *http.Request, op string, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(r.Context().Err(), context.Canceled) {
		// Client went away; nothing useful to return and not a server fault.
		annotate(r, "", "client_cancelled")
		writeError(w, 499, "client_closed_request", "client closed the request")
		return
	}
	// SQLSTATE class 22 (data exception: bad encoding, out-of-range value) means
	// the input could not be stored. That is the caller's problem, never a 5xx.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "22") {
		s.log.Warn(op+" rejected input", "request_id", RequestID(r), "sqlstate", pgErr.Code, "err", pgErr.Message)
		annotate(r, "", "invalid_request")
		writeError(w, http.StatusBadRequest, "invalid_request", "a value in the request cannot be stored")
		return
	}
	s.log.Error(op+" failed", "request_id", RequestID(r), "err", err)
	annotate(r, "", "server_error")
	writeError(w, http.StatusServiceUnavailable, "unavailable", "the service could not complete the request; retry with the same idempotency key")
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst any, limit int64) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		var tooBig *http.MaxBytesError
		switch {
		case errors.As(err, &tooBig):
			writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large", fmt.Sprintf("request body is larger than %d bytes", tooBig.Limit))
		case errors.Is(err, io.EOF):
			writeError(w, http.StatusBadRequest, "invalid_request", "request body is required")
		default:
			writeError(w, http.StatusBadRequest, "invalid_request", "malformed JSON body: "+err.Error())
		}
		return false
	}
	return true
}

// canonicalID accepts only the 36-character hyphenated UUID form, in any case,
// and returns it lower-cased. uuid.Parse alone also accepts urn:uuid:, braced
// and unhyphenated spellings; those would give one show several advisory-lock
// keys, and Postgres rejects some of them outright.
func canonicalID(raw string) (string, bool) {
	if len(raw) != 36 {
		return "", false
	}
	u, err := uuid.Parse(raw)
	if err != nil {
		return "", false
	}
	return u.String(), true
}

// printableText reports whether s is valid UTF-8 without control characters.
// Postgres TEXT rejects NUL and invalid UTF-8, which would otherwise surface as a 5xx.
func printableText(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func printableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to encode response")
		return
	}
	writeRaw(w, status, b)
}

func writeRaw(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body) //nolint:errcheck
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeRaw(w, status, (&store.Decline{HTTPStatus: status, Code: code, Message: msg}).Body())
}
