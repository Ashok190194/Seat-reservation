package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/seatreserve/seatreserve/internal/auth"
	"github.com/seatreserve/seatreserve/internal/db"
	"github.com/seatreserve/seatreserve/internal/metrics"
	"github.com/seatreserve/seatreserve/internal/store"
)

const adminToken = "test-admin"

func testServer(t *testing.T) (*httptest.Server, *atomic.Bool) {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping integration test")
	}
	ctx := context.Background()
	pool, err := db.Connect(ctx, url, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	m := metrics.New(pool, 10, log)
	st := store.New(pool, log, nil)
	var ready atomic.Bool
	ready.Store(true)
	srv := httptest.NewServer(New(st, auth.New("test-secret", adminToken), m, pool, log, &ready).Handler())
	t.Cleanup(srv.Close)
	return srv, &ready
}

type resp struct {
	code int
	body map[string]any
	hdr  http.Header
}

func call(t *testing.T, srv *httptest.Server, method, path, token, body string, hdr map[string]string) resp {
	t.Helper()
	req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	out := resp{code: r.StatusCode, hdr: r.Header}
	json.NewDecoder(r.Body).Decode(&out.body) //nolint:errcheck
	return out
}

func errCode(r resp) string {
	e, _ := r.body["error"].(map[string]any)
	c, _ := e["code"].(string)
	return c
}

func token(t *testing.T, srv *httptest.Server, user string) string {
	t.Helper()
	r := call(t, srv, "POST", "/auth/token", "", `{"user_id":"`+user+`"}`, nil)
	if r.code != 201 {
		t.Fatalf("token: %d %v", r.code, r.body)
	}
	return r.body["token"].(string)
}

func createShow(t *testing.T, srv *httptest.Server, body string) string {
	t.Helper()
	r := call(t, srv, "POST", "/shows", adminToken, body, nil)
	if r.code != 201 {
		t.Fatalf("create show: %d %v", r.code, r.body)
	}
	return r.body["id"].(string)
}

func TestIdentityComesFromTokenOnly(t *testing.T) {
	srv, _ := testServer(t)
	show := createShow(t, srv, `{"name":"id","seats":["A1","A2"],"price_paise":100}`)
	alice := token(t, srv, "alice")

	r := call(t, srv, "POST", "/shows/"+show+"/reserve", alice, `{"seats":["A1"],"user_id":"bob"}`, nil)
	if r.code != 201 || r.body["user_id"] != "alice" {
		t.Fatalf("spoofed user_id must be ignored: %d %v", r.code, r.body)
	}
	resID := r.body["reservation_id"].(string)

	bob := token(t, srv, "bob")
	if r := call(t, srv, "POST", "/reservations/"+resID+"/cancel", bob, "", nil); r.code != 403 || errCode(r) != "forbidden" {
		t.Fatalf("non-owner cancel: %d %v", r.code, r.body)
	}
	if r := call(t, srv, "GET", "/reservations/"+resID, bob, "", nil); r.code != 403 {
		t.Fatalf("non-owner get: %d", r.code)
	}
	if r := call(t, srv, "POST", "/reservations/"+resID+"/cancel", alice, "", nil); r.code != 200 || r.body["status"] != "cancelled" {
		t.Fatalf("owner cancel: %d %v", r.code, r.body)
	}
}

func TestAuthFailures(t *testing.T) {
	srv, _ := testServer(t)
	show := createShow(t, srv, `{"name":"auth","seats":["A1"],"price_paise":100}`)
	cases := []struct {
		name, method, path, token string
		want                      int
	}{
		{"reserve without token", "POST", "/shows/" + show + "/reserve", "", 401},
		{"reserve with garbage token", "POST", "/shows/" + show + "/reserve", "not.a.token", 401},
		{"create show as user", "POST", "/shows", token(t, srv, "alice"), 401},
		{"create show with wrong admin", "POST", "/shows", adminToken + "x", 401},
		{"list mine without token", "GET", "/users/me/reservations", "", 401},
	}
	for _, c := range cases {
		r := call(t, srv, c.method, c.path, c.token, `{"seats":["A1"],"name":"x","price_paise":1}`, nil)
		if r.code != c.want || errCode(r) != "unauthorized" {
			t.Errorf("%s: got %d %q", c.name, r.code, errCode(r))
		}
	}
}

func TestReserveValidation(t *testing.T) {
	srv, _ := testServer(t)
	show := createShow(t, srv, `{"name":"val","seats":["A1","A2","A3"],"price_paise":100,"per_user_limit":2}`)
	tok := token(t, srv, "val-user")
	path := "/shows/" + show + "/reserve"
	cases := []struct {
		name, body string
		hdr        map[string]string
		code       int
		errc       string
	}{
		{"empty body", "", nil, 400, "invalid_request"},
		{"malformed json", "{", nil, 400, "invalid_request"},
		{"no seats", `{"seats":[]}`, nil, 400, "invalid_request"},
		{"blank label", `{"seats":[" "]}`, nil, 400, "invalid_request"},
		{"key mismatch header vs body", `{"seats":["A1"],"idempotency_key":"a"}`, map[string]string{"Idempotency-Key": "b"}, 400, "invalid_request"},
		{"unknown seat", `{"seats":["Z9"]}`, nil, 422, "unknown_seat"},
		{"over limit in one request", `{"seats":["A1","A2","A3"]}`, nil, 409, "per_user_limit"},
	}
	for _, c := range cases {
		r := call(t, srv, "POST", path, tok, c.body, c.hdr)
		if r.code != c.code || errCode(r) != c.errc {
			t.Errorf("%s: got %d %q want %d %q", c.name, r.code, errCode(r), c.code, c.errc)
		}
	}
	if r := call(t, srv, "POST", "/shows", adminToken, `{"name":"x","seats":["A1"],"price_paise":12.5}`, nil); r.code != 400 {
		t.Errorf("float price: %d", r.code)
	}
	if r := call(t, srv, "POST", "/shows/not-a-uuid/reserve", tok, `{"seats":["A1"]}`, nil); r.code != 404 || errCode(r) != "show_not_found" {
		t.Errorf("bad show id: %d %q", r.code, errCode(r))
	}
	// Duplicates collapse, response seats are sorted.
	r := call(t, srv, "POST", path, tok, `{"seats":["A2","A1","A2"]}`, nil)
	if r.code != 201 {
		t.Fatalf("dup collapse: %d %v", r.code, r.body)
	}
	if seats, _ := json.Marshal(r.body["seats"]); string(seats) != `["A1","A2"]` {
		t.Errorf("seats = %s", seats)
	}
	if r.body["amount_paise"] != float64(200) {
		t.Errorf("amount_paise = %v", r.body["amount_paise"])
	}
}

func TestIdempotentReplayHeaderAndMetricsRoute(t *testing.T) {
	srv, _ := testServer(t)
	show := createShow(t, srv, `{"name":"idem","seats":["A1","A2"],"price_paise":100}`)
	tok := token(t, srv, "idem-user")
	path := "/shows/" + show + "/reserve"
	hdr := map[string]string{"Idempotency-Key": "k-" + show}
	first := call(t, srv, "POST", path, tok, `{"seats":["A1"]}`, hdr)
	second := call(t, srv, "POST", path, tok, `{"seats":["A1"]}`, hdr)
	if first.code != 201 || second.code != 201 || second.hdr.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("first=%d second=%d replayed=%q", first.code, second.code, second.hdr.Get("Idempotent-Replayed"))
	}
	if first.body["reservation_id"] != second.body["reservation_id"] {
		t.Fatal("replay returned a different reservation")
	}
	if r := call(t, srv, "POST", path, tok, `{"seats":["A2"]}`, hdr); r.code != 409 || errCode(r) != "idempotency_key_reused" {
		t.Fatalf("mismatch: %d %q", r.code, errCode(r))
	}
	if r := call(t, srv, "GET", "/metrics", "", "", nil); r.code != 200 {
		t.Fatalf("metrics: %d", r.code)
	}
	rid := call(t, srv, "GET", "/shows/"+show, "", "", map[string]string{"X-Request-ID": "trace-abc"})
	if rid.hdr.Get("X-Request-ID") != "trace-abc" {
		t.Fatalf("request id not echoed: %q", rid.hdr.Get("X-Request-ID"))
	}
}

func TestReadinessFailsClosedBeforeBootstrap(t *testing.T) {
	srv, ready := testServer(t)
	if r := call(t, srv, "GET", "/readyz", "", "", nil); r.code != 200 {
		t.Fatalf("ready: %d %v", r.code, r.body)
	}
	ready.Store(false)
	if r := call(t, srv, "GET", "/readyz", "", "", nil); r.code != 503 || r.body["status"] != "starting" {
		t.Fatalf("not ready: %d %v", r.code, r.body)
	}
	if r := call(t, srv, "GET", "/healthz", "", "", nil); r.code != 200 {
		t.Fatalf("liveness must stay up: %d", r.code)
	}
}

// metric reads one series from /metrics (0 if absent).
func metric(t *testing.T, srv *httptest.Server, series string) float64 {
	t.Helper()
	r, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, series+" ") {
			v, _ := strconv.ParseFloat(strings.TrimSpace(line[len(series):]), 64)
			return v
		}
	}
	return 0
}

func TestShowIDSpellingsAreOneShow(t *testing.T) {
	srv, _ := testServer(t)
	show := createShow(t, srv, `{"name":"spell","seats":["A1","A2","A3"],"price_paise":100}`)
	tok := token(t, srv, "spell-"+show[:8])
	hdr := map[string]string{"Idempotency-Key": "spell-" + show}

	first := call(t, srv, "POST", "/shows/"+strings.ToUpper(show)+"/reserve", tok, `{"seats":["A1"]}`, hdr)
	if first.code != 201 || first.body["show_id"] != show {
		t.Fatalf("upper-case id: %d %v (want 201 with canonical show_id)", first.code, first.body)
	}
	// The same key through the canonical spelling is the same request: a replay, not 409.
	again := call(t, srv, "POST", "/shows/"+show+"/reserve", tok, `{"seats":["A1"]}`, hdr)
	if again.code != 201 || again.hdr.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay across spellings: %d %v", again.code, again.body)
	}
	if r := call(t, srv, "GET", "/shows/"+strings.ToUpper(show), "", "", nil); r.code != 200 {
		t.Fatalf("GET upper-case id: %d", r.code)
	}
	// Spellings uuid.Parse accepts but Postgres rejects must be a clean 404, never a 5xx.
	for _, bad := range []string{"urn:uuid:" + show, "x" + show + "x", "{" + show + "}", strings.ReplaceAll(show, "-", "")} {
		if r := call(t, srv, "POST", "/shows/"+bad+"/reserve", tok, `{"seats":["A2"]}`, nil); r.code != 404 {
			t.Errorf("reserve %q: %d %v", bad, r.code, r.body)
		}
		if r := call(t, srv, "GET", "/shows/"+bad, "", "", nil); r.code != 404 {
			t.Errorf("get %q: %d", bad, r.code)
		}
	}
}

func TestPerUserLimitHoldsAcrossIDSpellingsOverHTTP(t *testing.T) {
	srv, _ := testServer(t)
	seats := make([]string, 12)
	for i := range seats {
		seats[i] = `"S` + strconv.Itoa(i+1) + `"`
	}
	show := createShow(t, srv, `{"name":"limit-spell","seats":[`+strings.Join(seats, ",")+`],"price_paise":100,"per_user_limit":4}`)
	tok := token(t, srv, "greedy-"+show[:8])
	spellings := []string{show, strings.ToUpper(show), strings.ToUpper(show[:8]) + show[8:]}
	var wg sync.WaitGroup
	var won atomic.Int32
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := call(t, srv, "POST", "/shows/"+spellings[i%3]+"/reserve", tok, `{"seats":["S`+strconv.Itoa(i+1)+`"]}`, nil)
			if r.code == 201 {
				won.Add(1)
			} else if r.code != 409 || errCode(r) != "per_user_limit" {
				t.Errorf("unexpected %d %v", r.code, r.body)
			}
		}(i)
	}
	wg.Wait()
	if won.Load() != 4 {
		t.Fatalf("user won %d seats on a limit-4 show", won.Load())
	}
}

func TestUnstorableInputIsAClientError(t *testing.T) {
	srv, _ := testServer(t)
	show := createShow(t, srv, `{"name":"bytes","seats":["A1","A2"],"price_paise":100}`)
	tok := token(t, srv, "bytes-"+show[:8])
	path := "/shows/" + show + "/reserve"
	cases := []struct {
		name, body string
		hdr        map[string]string
	}{
		{"NUL in body key", `{"seats":["A1"],"idempotency_key":"k\u0000x"}`, nil},
		{"invalid UTF-8 in header key", `{"seats":["A1"]}`, map[string]string{"Idempotency-Key": "k\xff"}},
		{"NUL in seat label", `{"seats":["A\u0000"]}`, nil},
		{"blank header key", `{"seats":["A1"]}`, map[string]string{"Idempotency-Key": "   "}},
		{"blank body key", `{"seats":["A1"],"idempotency_key":"  "}`, nil},
	}
	for _, c := range cases {
		if r := call(t, srv, "POST", path, tok, c.body, c.hdr); r.code != 400 || errCode(r) != "invalid_request" {
			t.Errorf("%s: got %d %q, want 400 invalid_request", c.name, r.code, errCode(r))
		}
	}
	if r := call(t, srv, "POST", "/shows", adminToken, `{"name":"bad\u0000name","seats":["A1"],"price_paise":1}`, nil); r.code != 400 {
		t.Errorf("NUL in show name: %d", r.code)
	}
}

func TestRepeatConfirmAndCancelDoNotInflateCounters(t *testing.T) {
	srv, _ := testServer(t)
	show := createShow(t, srv, `{"name":"counters","seats":["A1","A2"],"price_paise":100}`)
	tok := token(t, srv, "counters-"+show[:8])
	r := call(t, srv, "POST", "/shows/"+show+"/reserve", tok, `{"seats":["A1"]}`, nil)
	if r.code != 201 {
		t.Fatalf("reserve: %d %v", r.code, r.body)
	}
	id := r.body["reservation_id"].(string)
	confirmed := metric(t, srv, "reservations_confirmed_total")
	holds := metric(t, srv, "holds_confirmed_total")
	// Confirming an immediate sale is an idempotent no-op: 200, counters unchanged.
	for i := 0; i < 3; i++ {
		if r := call(t, srv, "POST", "/reservations/"+id+"/confirm", tok, "", nil); r.code != 200 {
			t.Fatalf("confirm: %d %v", r.code, r.body)
		}
	}
	if d := metric(t, srv, "reservations_confirmed_total") - confirmed; d != 0 {
		t.Errorf("reservations_confirmed_total moved by %v on no-op confirms", d)
	}
	if d := metric(t, srv, "holds_confirmed_total") - holds; d != 0 {
		t.Errorf("holds_confirmed_total moved by %v on no-op confirms", d)
	}
	cancelled := metric(t, srv, "reservations_cancelled_total")
	for i := 0; i < 3; i++ {
		if r := call(t, srv, "POST", "/reservations/"+id+"/cancel", tok, "", nil); r.code != 200 || r.body["status"] != "cancelled" {
			t.Fatalf("cancel: %d %v", r.code, r.body)
		}
	}
	if d := metric(t, srv, "reservations_cancelled_total") - cancelled; d != 1 {
		t.Errorf("reservations_cancelled_total moved by %v for one real cancel and two no-ops", d)
	}
	// Reservation ids get the same canonical treatment as show ids.
	if r := call(t, srv, "GET", "/reservations/"+strings.ToUpper(id), tok, "", nil); r.code != 200 {
		t.Errorf("GET upper-case reservation id: %d", r.code)
	}
	if r := call(t, srv, "POST", "/reservations/urn:uuid:"+id+"/cancel", tok, "", nil); r.code != 404 {
		t.Errorf("cancel urn id: %d", r.code)
	}
}
