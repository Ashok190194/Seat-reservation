package store

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/seatreserve/seatreserve/internal/db"
)

// These tests need a real Postgres: set DATABASE_URL (see Makefile `test`).
func testStore(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping integration test")
	}
	ctx := context.Background()
	pool, err := db.Connect(ctx, url, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return New(pool, log, nil), pool
}

func labels(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("S%03d", i+1)
	}
	return out
}

func mustShow(t *testing.T, s *Store, seats, limit, ttl int) *ShowState {
	t.Helper()
	st, err := s.CreateShow(context.Background(), CreateShowInput{Name: t.Name(), Seats: labels(seats), PricePaise: 25000, PerUserLimit: limit, HoldTTLSeconds: ttl})
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func assertInvariant(t *testing.T, s *Store, showID string) SeatCounts {
	t.Helper()
	st, err := s.GetShow(context.Background(), showID)
	if err != nil {
		t.Fatal(err)
	}
	if sum := st.Counts.Available + st.Counts.Held + st.Counts.Confirmed; sum != st.TotalSeats {
		t.Fatalf("invariant violated: %d+%d+%d=%d != %d", st.Counts.Available, st.Counts.Held, st.Counts.Confirmed, sum, st.TotalSeats)
	}
	return st.Counts
}

func TestHotSeatRaceHasExactlyOneWinner(t *testing.T) {
	s, _ := testStore(t)
	show := mustShow(t, s, 10, 4, 0)
	const racers = 200
	var wg sync.WaitGroup
	var mu sync.Mutex
	winners, taken, other := 0, 0, 0
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out, err := s.Reserve(context.Background(), ReserveInput{ShowID: show.ID, UserID: fmt.Sprintf("u%d", i), Seats: []string{"S001"},
				IdempotencyKey: fmt.Sprintf("k%d", i), RequestHash: "h"})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				t.Errorf("unexpected error: %v", err)
			case out.Reservation != nil:
				winners++
			case out.Decline != nil && out.Decline.Code == "seat_taken":
				taken++
			default:
				other++
			}
		}(i)
	}
	wg.Wait()
	if winners != 1 || taken != racers-1 || other != 0 {
		t.Fatalf("winners=%d taken=%d other=%d", winners, taken, other)
	}
	c := assertInvariant(t, s, show.ID)
	if c.Confirmed != 1 {
		t.Fatalf("confirmed=%d", c.Confirmed)
	}
}

func TestMultiSeatOppositeOrderNoDeadlockAllOrNothing(t *testing.T) {
	s, _ := testStore(t)
	show := mustShow(t, s, 4, 4, 0)
	// Caller is required to sort; the store relies on ORDER BY as well. Both
	// requests want the same two seats: exactly one wins both, the other gets neither.
	var wg sync.WaitGroup
	results := make([]*ReserveOutcome, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out, err := s.Reserve(context.Background(), ReserveInput{ShowID: show.ID, UserID: fmt.Sprintf("u%d", i), Seats: []string{"S001", "S002"}})
			if err != nil {
				t.Error(err)
				return
			}
			results[i] = out
		}(i)
	}
	wg.Wait()
	wins := 0
	for _, r := range results {
		if r != nil && r.Reservation != nil {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("wins=%d", wins)
	}
	c := assertInvariant(t, s, show.ID)
	if c.Confirmed != 2 || c.Available != 2 {
		t.Fatalf("counts=%+v", c)
	}
}

func TestPerUserLimitUnderConcurrency(t *testing.T) {
	s, _ := testStore(t)
	show := mustShow(t, s, 20, 4, 0)
	var wg sync.WaitGroup
	var mu sync.Mutex
	won, limited := 0, 0
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out, err := s.Reserve(context.Background(), ReserveInput{ShowID: show.ID, UserID: "greedy", Seats: []string{fmt.Sprintf("S%03d", i+1)}})
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if out.Reservation != nil {
				won++
			} else if out.Decline != nil && out.Decline.Code == "per_user_limit" {
				limited++
			}
		}(i)
	}
	wg.Wait()
	if won != 4 || limited != 6 {
		t.Fatalf("won=%d limited=%d", won, limited)
	}
	assertInvariant(t, s, show.ID)
}

func TestIdempotencyExactlyOnceAndMismatch(t *testing.T) {
	s, _ := testStore(t)
	show := mustShow(t, s, 5, 4, 0)
	in := ReserveInput{ShowID: show.ID, UserID: "alice", Seats: []string{"S001"}, IdempotencyKey: "key-1", RequestHash: "hash-A"}

	// 50 concurrent requests with the same key: one creates, the rest replay.
	var wg sync.WaitGroup
	var mu sync.Mutex
	created, replayed := 0, 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := s.Reserve(context.Background(), in)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if out.Replayed {
				replayed++
			} else if out.Reservation != nil {
				created++
			}
		}()
	}
	wg.Wait()
	if created != 1 || replayed != 49 {
		t.Fatalf("created=%d replayed=%d", created, replayed)
	}
	// Same key, different body.
	in2 := in
	in2.Seats = []string{"S002"}
	in2.RequestHash = "hash-B"
	out, err := s.Reserve(context.Background(), in2)
	if err != nil || out.Decline == nil || out.Decline.Code != "idempotency_key_reused" {
		t.Fatalf("expected idempotency_key_reused, got %+v err=%v", out, err)
	}
	// Same key, different user: keys are scoped per user, so bob gets his own decision (seat taken).
	in3 := in
	in3.UserID = "bob"
	out, err = s.Reserve(context.Background(), in3)
	if err != nil || out.Decline == nil || out.Decline.Code != "seat_taken" {
		t.Fatalf("expected seat_taken for bob, got %+v err=%v", out, err)
	}
	c := assertInvariant(t, s, show.ID)
	if c.Confirmed != 1 {
		t.Fatalf("confirmed=%d", c.Confirmed)
	}
}

func TestDeclinesAreReplayedToo(t *testing.T) {
	s, _ := testStore(t)
	show := mustShow(t, s, 2, 4, 0)
	if _, err := s.Reserve(context.Background(), ReserveInput{ShowID: show.ID, UserID: "alice", Seats: []string{"S001"}}); err != nil {
		t.Fatal(err)
	}
	in := ReserveInput{ShowID: show.ID, UserID: "bob", Seats: []string{"S001"}, IdempotencyKey: "k", RequestHash: "h"}
	first, _ := s.Reserve(context.Background(), in)
	if first.Decline == nil || first.Decline.Code != "seat_taken" {
		t.Fatalf("first=%+v", first)
	}
	// Alice frees the seat; bob's retry with the same key still sees his original 409.
	second, _ := s.Reserve(context.Background(), in)
	if !second.Replayed || second.ReplayStatus != 409 {
		t.Fatalf("second=%+v", second)
	}
}

func TestHoldExpiryAndNoResurrection(t *testing.T) {
	s, pool := testStore(t)
	show := mustShow(t, s, 3, 4, 60)
	ctx := context.Background()
	out, err := s.Reserve(ctx, ReserveInput{ShowID: show.ID, UserID: "alice", Seats: []string{"S001"}})
	if err != nil || out.Reservation == nil || out.Reservation.Status != StatusHeld {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	hold := out.Reservation
	c := assertInvariant(t, s, show.ID)
	if c.Held != 1 {
		t.Fatalf("held=%d", c.Held)
	}
	// Force the hold into the past, then sweep.
	if _, err := pool.Exec(ctx, `UPDATE seats SET hold_expires_at = now() - interval '1 second' WHERE reservation_id = $1`, hold.ID); err != nil {
		t.Fatal(err)
	}
	seats, reservations, err := s.SweepExpired(ctx, 100)
	if err != nil || seats != 1 || reservations != 1 {
		t.Fatalf("seats=%d reservations=%d err=%v", seats, reservations, err)
	}
	c = assertInvariant(t, s, show.ID)
	if c.Available != 3 {
		t.Fatalf("available=%d", c.Available)
	}
	// Confirm after expiry must fail.
	if _, err := s.Confirm(ctx, hold.ID, "alice"); err == nil {
		t.Fatal("confirm of expired hold should fail")
	}
	// Bob takes the seat; alice's stale cancel must not free it.
	out2, err := s.Reserve(ctx, ReserveInput{ShowID: show.ID, UserID: "bob", Seats: []string{"S001"}})
	if err != nil || out2.Reservation == nil {
		t.Fatalf("bob: %+v %v", out2, err)
	}
	if _, err := s.Cancel(ctx, hold.ID, "alice"); err == nil {
		t.Fatal("cancel of expired hold should be declined")
	}
	c = assertInvariant(t, s, show.ID)
	if c.Held != 1 || c.Available != 2 {
		t.Fatalf("counts=%+v", c)
	}
	// Bob confirms, then only bob can cancel.
	if _, err := s.Confirm(ctx, out2.Reservation.ID, "bob"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Cancel(ctx, out2.Reservation.ID, "alice"); err != ErrForbidden {
		t.Fatalf("expected ErrForbidden, got %v", err)
	}
	res, err := s.Cancel(ctx, out2.Reservation.ID, "bob")
	if err != nil || res.Status != StatusCancelled {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	c = assertInvariant(t, s, show.ID)
	if c.Available != 3 {
		t.Fatalf("available=%d", c.Available)
	}
}

func TestSweeperAndReserveDoNotDeadlock(t *testing.T) {
	s, pool := testStore(t)
	show := mustShow(t, s, 50, 50, 1)
	ctx := context.Background()
	// Fill the hall with holds, expire them all, then hammer reserves while sweeping.
	for i := 0; i < 50; i++ {
		if _, err := s.Reserve(ctx, ReserveInput{ShowID: show.ID, UserID: "filler", Seats: []string{fmt.Sprintf("S%03d", i+1)}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE seats SET hold_expires_at = now() - interval '1 second' WHERE show_id = $1`, show.ID); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	stop := time.Now().Add(1500 * time.Millisecond)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(stop) {
			if _, _, err := s.SweepExpired(ctx, 7); err != nil {
				t.Error("sweep:", err)
				return
			}
		}
	}()
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; time.Now().Before(stop); i++ {
				a, b := (i*7+g)%50, (i*13+g*3)%50
				if a == b {
					b = (b + 1) % 50
				}
				seats := []string{fmt.Sprintf("S%03d", min(a, b)+1), fmt.Sprintf("S%03d", max(a, b)+1)}
				if _, err := s.Reserve(ctx, ReserveInput{ShowID: show.ID, UserID: fmt.Sprintf("g%d-%d", g, i), Seats: seats}); err != nil {
					t.Error("reserve:", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	assertInvariant(t, s, show.ID)
}
