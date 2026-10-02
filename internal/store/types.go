// Package store holds every database transaction. All the correctness
// guarantees (no double-sell, per-user limit, exactly-once idempotency) live here.
package store

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Seat/reservation statuses.
const (
	StatusAvailable = "available"
	StatusHeld      = "held"
	StatusConfirmed = "confirmed"
	StatusCancelled = "cancelled"
	StatusExpired   = "expired"
)

type Show struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	PricePaise     int64     `json:"price_paise"`
	PerUserLimit   int       `json:"per_user_limit"`
	HoldTTLSeconds int       `json:"hold_ttl_seconds"`
	TotalSeats     int       `json:"total_seats"`
	CreatedAt      time.Time `json:"created_at"`
}

type SeatCounts struct {
	Available int `json:"available"`
	Held      int `json:"held"`
	Confirmed int `json:"confirmed"`
}

type SeatState struct {
	Label  string `json:"label"`
	Status string `json:"status"`
}

type ShowState struct {
	Show
	Counts SeatCounts  `json:"counts"`
	Seats  []SeatState `json:"seats"`
}

type Reservation struct {
	ID          string     `json:"reservation_id"`
	ShowID      string     `json:"show_id"`
	UserID      string     `json:"user_id"`
	Seats       []string   `json:"seats"`
	AmountPaise int64      `json:"amount_paise"`
	Status      string     `json:"status"`
	ExpiresAt   *time.Time `json:"expires_at"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// Decline is a domain outcome, not a failure: the request was understood and
// the answer is "no". It always maps to a 4xx.
type Decline struct {
	HTTPStatus int      `json:"-"`
	Code       string   `json:"code"`
	Message    string   `json:"message"`
	Seats      []string `json:"seats,omitempty"`
}

func (d *Decline) Error() string { return fmt.Sprintf("%s: %s", d.Code, d.Message) }

// ErrorBody is the wire shape for every 4xx/5xx response.
type ErrorBody struct {
	Error *Decline `json:"error"`
}

func (d *Decline) Body() []byte {
	b, _ := json.Marshal(ErrorBody{Error: d})
	return b
}

func seatTaken(seats []string) *Decline {
	return &Decline{HTTPStatus: http.StatusConflict, Code: "seat_taken",
		Message: "one or more requested seats are already held or confirmed", Seats: seats}
}

func perUserLimit(limit, holding, requested int) *Decline {
	return &Decline{HTTPStatus: http.StatusConflict, Code: "per_user_limit",
		Message: fmt.Sprintf("per-user limit is %d seats for this show; you hold %d and asked for %d more", limit, holding, requested)}
}

// tooManySeats is the lock-free early check: no amount of waiting makes the
// request fit, so it is declined before any lock is taken.
func tooManySeats(limit, requested int) *Decline {
	return &Decline{HTTPStatus: http.StatusConflict, Code: "per_user_limit",
		Message: fmt.Sprintf("per-user limit is %d seats for this show; this request asks for %d", limit, requested)}
}

func unknownSeat(seats []string) *Decline {
	return &Decline{HTTPStatus: http.StatusUnprocessableEntity, Code: "unknown_seat",
		Message: "one or more requested seats do not exist in this show", Seats: seats}
}

func showNotFound() *Decline {
	return &Decline{HTTPStatus: http.StatusNotFound, Code: "show_not_found", Message: "show does not exist"}
}

func idempotencyMismatch() *Decline {
	return &Decline{HTTPStatus: http.StatusConflict, Code: "idempotency_key_reused",
		Message: "this idempotency key was already used with a different request body"}
}

// ReserveOutcome is exactly one of: a fresh reservation (201), a replayed
// stored response, or a decline.
type ReserveOutcome struct {
	Reservation *Reservation
	Decline     *Decline

	Replayed     bool
	ReplayStatus int
	ReplayBody   []byte
}
