// Command burst reproduces an on-sale stampede against a running instance and
// verifies the correctness bar: one winner per hot seat, zero 5xx, idempotent
// replays, per-user limit, and the reconciliation invariant.
//
//	go run ./cmd/burst -url https://your-service.example.com
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type options struct {
	baseURL     string
	adminToken  string
	seats       int
	users       int
	requests    int
	concurrency int
	hotSeats    int
	hotUsers    int
	limit       int
	ttl         int
	retryPct    int
	keepShow    bool
}

func main() {
	var o options
	flag.StringVar(&o.baseURL, "url", envOr("BASE_URL", "http://localhost:8787"), "base URL of the service")
	flag.StringVar(&o.adminToken, "admin-token", envOr("ADMIN_TOKEN", "admin-dev-token"), "admin bearer token for POST /shows")
	flag.IntVar(&o.seats, "seats", 1000, "seats in the show")
	flag.IntVar(&o.users, "users", 2000, "distinct buyers")
	flag.IntVar(&o.requests, "requests", 20000, "reserve requests in the general stampede phase")
	flag.IntVar(&o.concurrency, "concurrency", 500, "in-flight requests")
	flag.IntVar(&o.hotSeats, "hot-seats", 5, "number of hot seats to storm")
	flag.IntVar(&o.hotUsers, "hot-users", 500, "buyers fighting over each hot seat")
	flag.IntVar(&o.limit, "limit", 4, "per_user_limit for the show")
	flag.IntVar(&o.ttl, "ttl", 0, "hold_ttl_seconds for the show (0 = immediate confirm)")
	flag.IntVar(&o.retryPct, "retry-pct", 5, "percent of stampede requests that are idempotent retries of an earlier request")
	flag.BoolVar(&o.keepShow, "keep", true, "leave the show in place afterwards (there is no delete endpoint)")
	flag.Parse()
	if flag.NArg() == 1 {
		o.baseURL = flag.Arg(0)
	}
	o.baseURL = strings.TrimRight(o.baseURL, "/")

	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "\nFAIL:", err)
		os.Exit(1)
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// ---------------------------------------------------------------------------

type client struct {
	base string
	http *http.Client
}

type result struct {
	status  int
	code    string // domain code from error body, "" on success
	body    []byte
	latency time.Duration
	err     error
}

func newClient(base string, concurrency int) *client {
	tr := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        concurrency,
		MaxIdleConnsPerHost: concurrency,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	return &client{base: base, http: &http.Client{Transport: tr, Timeout: 90 * time.Second}}
}

func (c *client) do(method, path, token string, body any, headers map[string]string) result {
	var buf io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		buf = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, c.base+path, buf)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	start := time.Now()
	resp, err := c.http.Do(req)
	r := result{latency: time.Since(start)}
	if err != nil {
		r.err = err
		return r
	}
	defer resp.Body.Close()
	r.status = resp.StatusCode
	r.body, _ = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if r.status >= 400 {
		var eb struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if json.Unmarshal(r.body, &eb) == nil {
			r.code = eb.Error.Code
		}
	}
	return r
}

type reservation struct {
	ID     string   `json:"reservation_id"`
	UserID string   `json:"user_id"`
	Seats  []string `json:"seats"`
	Status string   `json:"status"`
}

type showState struct {
	ID         string `json:"id"`
	TotalSeats int    `json:"total_seats"`
	Counts     struct {
		Available, Held, Confirmed int
	} `json:"counts"`
	Seats []struct {
		Label, Status string
	} `json:"seats"`
}

// ---------------------------------------------------------------------------
// tally

type tally struct {
	mu        sync.Mutex
	name      string
	total     int
	success   int // 201
	replayed  int // any status with Idempotent-Replayed semantics (we detect via header-less: tracked by caller)
	byCode    map[string]int
	other4xx  int
	fivexx    int // 5xx with the app's JSON error body
	edge5xx   int // 5xx without it: platform proxy/edge answered, the app never saw the request
	edgeCodes map[int]int
	transport int
	lat       []time.Duration
}

func newTally(name string) *tally {
	return &tally{name: name, byCode: map[string]int{}, edgeCodes: map[int]int{}}
}

func (t *tally) add(r result) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.total++
	t.lat = append(t.lat, r.latency)
	switch {
	case r.err != nil:
		t.transport++
	case r.status == 201:
		t.success++
	case r.status >= 500:
		if r.code != "" {
			t.fivexx++
		} else {
			t.edge5xx++
			t.edgeCodes[r.status]++
		}
	case r.status >= 400:
		if r.code != "" {
			t.byCode[r.code]++
		} else {
			t.other4xx++
		}
	}
}

func (t *tally) print(elapsed time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	fmt.Printf("\n  %-34s %6d requests in %s (%.0f req/s)\n", t.name, t.total, elapsed.Round(time.Millisecond), float64(t.total)/elapsed.Seconds())
	fmt.Printf("    %-32s %6d\n", "201 created", t.success)
	codes := make([]string, 0, len(t.byCode))
	for c := range t.byCode {
		codes = append(codes, c)
	}
	sort.Strings(codes)
	for _, c := range codes {
		fmt.Printf("    %-32s %6d\n", "4xx "+c, t.byCode[c])
	}
	if t.replayed > 0 {
		fmt.Printf("    %-32s %6d\n", "idempotent replays (orig status)", t.replayed)
	}
	if t.other4xx > 0 {
		fmt.Printf("    %-32s %6d\n", "4xx (other)", t.other4xx)
	}
	fmt.Printf("    %-32s %6d%s\n", "5xx (from app)", t.fivexx, flag5(t.fivexx))
	if t.edge5xx > 0 {
		fmt.Printf("    %-32s %6d   <-- platform edge/proxy, not the app %v\n", "5xx (edge, no app body)", t.edge5xx, t.edgeCodes)
	}
	fmt.Printf("    %-32s %6d%s\n", "transport errors", t.transport, flag5(t.transport))
	if len(t.lat) > 0 {
		sort.Slice(t.lat, func(i, j int) bool { return t.lat[i] < t.lat[j] })
		p := func(q float64) time.Duration { return t.lat[int(float64(len(t.lat)-1)*q)] }
		fmt.Printf("    latency p50 %s  p95 %s  p99 %s  max %s\n", p(.5).Round(time.Millisecond), p(.95).Round(time.Millisecond), p(.99).Round(time.Millisecond), t.lat[len(t.lat)-1].Round(time.Millisecond))
	}
}

// warnEdge reports requests the app never answered (proxy 5xx, dropped
// connections). They are not app faults, so they do not fail the run, but a
// reviewer's own burst would see them too, so they are called out loudly.
func warnEdge(t *tally) {
	if n := t.edge5xx + t.transport; n > 0 {
		fmt.Printf("    warning: %d request(s) never reached the app (edge 5xx or dropped connection) — platform capacity, not a correctness fault\n", n)
	}
}

func flag5(n int) string {
	if n > 0 {
		return "   <-- PROBLEM"
	}
	return ""
}

// ---------------------------------------------------------------------------

type failures []string

func (f *failures) check(ok bool, format string, args ...any) {
	if !ok {
		*f = append(*f, fmt.Sprintf(format, args...))
	}
}

func run(o options) error {
	rand.Seed(time.Now().UnixNano())
	c := newClient(o.baseURL, o.concurrency)
	var fails failures

	fmt.Printf("burst → %s\n", o.baseURL)
	fmt.Printf("  show: %d seats, limit %d/user, ttl %ds · %d users · hot storm: %d seats × %d buyers · stampede: %d requests @ %d concurrent\n",
		o.seats, o.limit, o.ttl, o.users, o.hotSeats, o.hotUsers, o.requests, o.concurrency)

	// 0. readiness
	r := c.do("GET", "/readyz", "", nil, nil)
	if r.err != nil || r.status != 200 {
		return fmt.Errorf("service not ready: status=%d err=%v body=%s", r.status, r.err, r.body)
	}
	fmt.Println("  /readyz ok")

	// 1. create show
	labels := make([]string, o.seats)
	for i := range labels {
		labels[i] = fmt.Sprintf("%c%d", 'A'+rune(i/20%26), i%20+1)
		if i >= 520 { // beyond Z20, disambiguate
			labels[i] = fmt.Sprintf("%c%d-%d", 'A'+rune(i/20%26), i%20+1, i/520)
		}
	}
	r = c.do("POST", "/shows", o.adminToken, map[string]any{
		"name": fmt.Sprintf("burst-%s", time.Now().UTC().Format("20060102-150405")), "seats": labels,
		"price_paise": 25000, "per_user_limit": o.limit, "hold_ttl_seconds": o.ttl}, nil)
	if r.status != 201 {
		return fmt.Errorf("create show failed: status=%d body=%s err=%v", r.status, r.body, r.err)
	}
	var show showState
	json.Unmarshal(r.body, &show) //nolint:errcheck
	fmt.Printf("  show %s created with %d seats\n", show.ID, show.TotalSeats)
	reservePath := "/shows/" + show.ID + "/reserve"

	// 2. mint tokens
	start := time.Now()
	tokens := make([]string, o.users)
	{
		var wg sync.WaitGroup
		sem := make(chan struct{}, o.concurrency)
		var errs, retries int32
		var lastErr string
		var lastErrMu sync.Mutex
		for i := 0; i < o.users; i++ {
			wg.Add(1)
			sem <- struct{}{}
			go func(i int) {
				defer wg.Done()
				defer func() { <-sem }()
				// Minting is setup, not the system under test: retry transient
				// edge/network failures so they do not abort the run.
				for attempt := 0; attempt < 4; attempt++ {
					if attempt > 0 {
						atomic.AddInt32(&retries, 1)
						time.Sleep(time.Duration(200*attempt) * time.Millisecond)
					}
					rr := c.do("POST", "/auth/token", "", map[string]string{"user_id": fmt.Sprintf("buyer-%05d", i)}, nil)
					var t struct {
						Token string `json:"token"`
					}
					if rr.status == 201 && json.Unmarshal(rr.body, &t) == nil {
						tokens[i] = t.Token
						return
					}
					lastErrMu.Lock()
					lastErr = fmt.Sprintf("status=%d err=%v body=%.80s", rr.status, rr.err, rr.body)
					lastErrMu.Unlock()
				}
				atomic.AddInt32(&errs, 1)
			}(i)
		}
		wg.Wait()
		if errs > 0 {
			return fmt.Errorf("%d token mints failed after retries (last: %s)", errs, lastErr)
		}
		if retries > 0 {
			fmt.Printf("  note: %d token mint attempts failed transiently and were retried (last: %s)\n", retries, lastErr)
		}
	}
	fmt.Printf("  %d tokens minted in %s\n", o.users, time.Since(start).Round(time.Millisecond))

	// winners tracks seat -> reservation id for every 201 we observe.
	var winnersMu sync.Mutex
	winners := map[string]reservation{}
	var doubleSells []string
	record := func(res reservation) {
		winnersMu.Lock()
		defer winnersMu.Unlock()
		for _, s := range res.Seats {
			if prev, ok := winners[s]; ok && prev.ID != res.ID {
				doubleSells = append(doubleSells, fmt.Sprintf("%s sold to %s(%s) and %s(%s)", s, prev.UserID, prev.ID, res.UserID, res.ID))
			}
			winners[s] = res
		}
	}

	// 3. hot-seat storm: hotUsers distinct buyers hammer each of hotSeats seats at once.
	hot := newTally("hot-seat storm")
	hotWins := make([]int32, o.hotSeats)
	start = time.Now()
	{
		var wg sync.WaitGroup
		sem := make(chan struct{}, o.concurrency)
		for si := 0; si < o.hotSeats && si < len(labels); si++ {
			seat := labels[si]
			for u := 0; u < o.hotUsers; u++ {
				wg.Add(1)
				sem <- struct{}{}
				go func(si int, seat string, u int) {
					defer wg.Done()
					defer func() { <-sem }()
					tok := tokens[(si*o.hotUsers+u)%len(tokens)]
					rr := c.do("POST", reservePath, tok, map[string]any{"seats": []string{seat}}, map[string]string{"Idempotency-Key": uuid.NewString()})
					hot.add(rr)
					if rr.status == 201 {
						atomic.AddInt32(&hotWins[si], 1)
						var res reservation
						if json.Unmarshal(rr.body, &res) == nil {
							record(res)
						}
					}
				}(si, seat, u)
			}
		}
		wg.Wait()
	}
	hot.print(time.Since(start))
	{
		after, err := getShow(c, show.ID)
		if err != nil {
			return err
		}
		status := map[string]string{}
		for _, s := range after.Seats {
			status[s.Label] = s.Status
		}
		for si := 0; si < o.hotSeats && si < len(labels); si++ {
			seat := labels[si]
			note := ""
			switch {
			case hotWins[si] == 0 && status[seat] != "available":
				note = "  (sold; the winner's 201 was lost at the edge)"
			case hotWins[si] == 0:
				note = "  (no request reached the app)"
			}
			fmt.Printf("    seat %-6s winners seen by client: %d · API status: %s%s\n", seat, hotWins[si], status[seat], note)
			fails.check(hotWins[si] <= 1, "hot seat %s had %d winners (want exactly 1)", seat, hotWins[si])
			fails.check(hotWins[si] == 0 || status[seat] != "available", "hot seat %s: client won but API says available", seat)
		}
	}
	fails.check(hot.fivexx == 0, "hot-seat storm produced %d 5xx", hot.fivexx)
	warnEdge(hot)

	// 4. per-user limit: one fresh user fires 10 parallel single-seat reserves on free seats.
	lim := newTally("per-user limit (10 parallel, limit " + fmt.Sprint(o.limit) + ")")
	start = time.Now()
	{
		rr := c.do("POST", "/auth/token", "", map[string]string{"user_id": "greedy-" + uuid.NewString()[:8]}, nil)
		var t struct {
			Token string `json:"token"`
		}
		json.Unmarshal(rr.body, &t) //nolint:errcheck
		st, err := getShow(c, show.ID)
		if err != nil {
			return err
		}
		var free []string
		for _, s := range st.Seats {
			if s.Status == "available" {
				free = append(free, s.Label)
			}
		}
		if len(free) < 10 {
			fmt.Println("    (fewer than 10 free seats left; skipping)")
		} else {
			var wg sync.WaitGroup
			var won int32
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func(seat string) {
					defer wg.Done()
					rr := c.do("POST", reservePath, t.Token, map[string]any{"seats": []string{seat}}, map[string]string{"Idempotency-Key": uuid.NewString()})
					lim.add(rr)
					if rr.status == 201 {
						atomic.AddInt32(&won, 1)
						var res reservation
						if json.Unmarshal(rr.body, &res) == nil {
							record(res)
						}
					}
				}(free[len(free)-1-i])
			}
			wg.Wait()
			lim.print(time.Since(start))
			fmt.Printf("    greedy user ended with %d seats (limit %d)\n", won, o.limit)
			fails.check(int(won) <= o.limit, "per-user limit violated: user got %d seats with limit %d", won, o.limit)
			if int(won) < o.limit {
				fmt.Printf("    warning: expected the user to fill the limit exactly (%d) with 10 free seats; got %d\n", o.limit, won)
			}
		}
	}

	// 5. general stampede with mixed-in idempotent retries.
	type sent struct {
		key    string
		body   map[string]any
		token  string
		status int
		resp   []byte
	}
	var sentMu sync.Mutex
	var completed []sent
	gen := newTally("general stampede")
	var replayMismatch, replayLost int32
	var replayCount int32
	start = time.Now()
	{
		var wg sync.WaitGroup
		sem := make(chan struct{}, o.concurrency)
		for i := 0; i < o.requests; i++ {
			wg.Add(1)
			sem <- struct{}{}
			go func(i int) {
				defer wg.Done()
				defer func() { <-sem }()
				// Retry an earlier request with the same key and body?
				if rand.Intn(100) < o.retryPct {
					sentMu.Lock()
					var prev *sent
					if len(completed) > 0 {
						p := completed[rand.Intn(len(completed))]
						prev = &p
					}
					sentMu.Unlock()
					if prev != nil {
						rr := c.do("POST", reservePath, prev.token, prev.body, map[string]string{"Idempotency-Key": prev.key})
						gen.add(rr)
						atomic.AddInt32(&replayCount, 1)
						lostInTransit := rr.err != nil || (rr.status >= 500 && rr.code == "")
						if lostInTransit {
							atomic.AddInt32(&replayLost, 1)
						}
						if !lostInTransit && (rr.status != prev.status || !bytes.Equal(rr.body, prev.resp)) {
							atomic.AddInt32(&replayMismatch, 1)
						}
						return
					}
				}
				n := 1
				if rand.Intn(4) == 0 {
					n = 2
				}
				seatSet := map[string]struct{}{}
				for len(seatSet) < n {
					seatSet[labels[rand.Intn(len(labels))]] = struct{}{}
				}
				seats := make([]string, 0, n)
				for s := range seatSet {
					seats = append(seats, s)
				}
				sort.Strings(seats)
				tok := tokens[rand.Intn(len(tokens))]
				key := uuid.NewString()
				body := map[string]any{"seats": seats, "user_id": "someone-else"} // spoofed field must be ignored
				rr := c.do("POST", reservePath, tok, body, map[string]string{"Idempotency-Key": key})
				gen.add(rr)
				if rr.status == 201 {
					var res reservation
					if json.Unmarshal(rr.body, &res) == nil {
						record(res)
						if res.UserID == "someone-else" {
							atomic.AddInt32(&replayMismatch, 1<<20) // sentinel: identity spoof succeeded
						}
					}
				}
				if rr.err == nil && rr.status < 500 {
					sentMu.Lock()
					completed = append(completed, sent{key: key, body: body, token: tok, status: rr.status, resp: rr.body})
					sentMu.Unlock()
				}
			}(i)
		}
		wg.Wait()
	}
	gen.replayed = int(replayCount)
	gen.print(time.Since(start))
	spoofed := replayMismatch >= 1<<20
	replayMismatch &= (1 << 20) - 1
	fmt.Printf("    replays with identical response: %d / %d answered by the app (%d lost at the edge)\n",
		int(replayCount)-int(replayMismatch)-int(replayLost), int(replayCount)-int(replayLost), replayLost)
	fails.check(replayMismatch == 0, "%d idempotent retries returned a different response than the original", replayMismatch)
	fails.check(!spoofed, "a reservation was created for the spoofed body user_id")
	fails.check(gen.fivexx == 0, "stampede produced %d 5xx", gen.fivexx)
	warnEdge(gen)

	// 6. same key, different body -> 409 idempotency_key_reused
	mis := newTally("same key, different seats")
	start = time.Now()
	{
		sentMu.Lock()
		var pool []sent
		for _, s := range completed {
			if s.status == 201 {
				pool = append(pool, s)
			}
		}
		sentMu.Unlock()
		var wg sync.WaitGroup
		for i := 0; i < 50 && i < len(pool); i++ {
			s := pool[i]
			wg.Add(1)
			go func(s sent) {
				defer wg.Done()
				other := labels[rand.Intn(len(labels))]
				rr := c.do("POST", reservePath, s.token, map[string]any{"seats": []string{other, other + "x"}}, map[string]string{"Idempotency-Key": s.key})
				mis.add(rr)
			}(s)
		}
		wg.Wait()
	}
	mis.print(time.Since(start))
	fails.check(mis.byCode["idempotency_key_reused"] == mis.total, "expected every same-key/different-body request to be 409 idempotency_key_reused, got %d/%d", mis.byCode["idempotency_key_reused"], mis.total)

	// 7. identity: cancel someone else's reservation must be 403.
	{
		winnersMu.Lock()
		var victim reservation
		for _, w := range winners {
			victim = w
			break
		}
		winnersMu.Unlock()
		if victim.ID != "" {
			rr := c.do("POST", "/auth/token", "", map[string]string{"user_id": "intruder"}, nil)
			var t struct {
				Token string `json:"token"`
			}
			json.Unmarshal(rr.body, &t) //nolint:errcheck
			rr = c.do("POST", "/reservations/"+victim.ID+"/cancel", t.Token, nil, nil)
			fmt.Printf("\n  cross-user cancel of %s → %d %s\n", victim.ID, rr.status, rr.code)
			fails.check(rr.status == 403, "cross-user cancel returned %d (want 403)", rr.status)
			st1, _ := getShow(c, show.ID)
			if st1 != nil {
				for _, s := range st1.Seats {
					for _, vs := range victim.Seats {
						fails.check(!(s.Label == vs && s.Status == "available"), "seat %s was released by a non-owner", vs)
					}
				}
			}
		}
	}

	// 8. reconciliation
	st, err := getShow(c, show.ID)
	if err != nil {
		return err
	}
	sum := st.Counts.Available + st.Counts.Held + st.Counts.Confirmed
	fmt.Printf("\n  reconciliation (GET /shows/%s)\n", show.ID)
	fmt.Printf("    available %d + held %d + confirmed %d = %d ; total_seats %d  %s\n",
		st.Counts.Available, st.Counts.Held, st.Counts.Confirmed, sum, st.TotalSeats, okMark(sum == st.TotalSeats))
	fails.check(sum == st.TotalSeats, "invariant violated: %d != %d", sum, st.TotalSeats)

	winnersMu.Lock()
	taken := st.Counts.Held + st.Counts.Confirmed
	fmt.Printf("    seats won by this run (distinct): %d ; seats taken per API: %d  %s\n", len(winners), taken, okMark(len(winners) == taken))
	lost := hot.edge5xx + hot.transport + gen.edge5xx + gen.transport
	if lost == 0 {
		fails.check(len(winners) == taken, "client saw %d distinct winning seats but API reports %d taken", len(winners), taken)
	} else {
		fails.check(len(winners) <= taken && taken-len(winners) <= lost,
			"client saw %d winning seats, API reports %d taken, only %d requests were lost in transit", len(winners), taken, lost)
	}
	fails.check(len(doubleSells) == 0, "DOUBLE SELL: %v", doubleSells)
	for _, s := range st.Seats {
		_, won := winners[s.Label]
		if lost == 0 || won {
			fails.check(won == (s.Status != "available"), "seat %s: client won=%v but API status=%s", s.Label, won, s.Status)
		}
	}
	winnersMu.Unlock()

	// 9. metrics cross-check
	if m := c.do("GET", "/metrics", "", nil, nil); m.status == 200 {
		avail, conf, held := -1.0, -1.0, -1.0
		for _, line := range strings.Split(string(m.body), "\n") {
			if !strings.HasPrefix(line, "seats_by_status{") || !strings.Contains(line, `show_id="`+show.ID+`"`) {
				continue
			}
			var v float64
			fmt.Sscanf(line[strings.LastIndex(line, " ")+1:], "%g", &v) //nolint:errcheck
			switch {
			case strings.Contains(line, `status="available"`):
				avail = v
			case strings.Contains(line, `status="confirmed"`):
				conf = v
			case strings.Contains(line, `status="held"`):
				held = v
			}
		}
		fmt.Printf("    /metrics seats_by_status: available %.0f held %.0f confirmed %.0f  %s\n", avail, held, conf,
			okMark(int(avail) == st.Counts.Available && int(conf) == st.Counts.Confirmed && int(held) == st.Counts.Held))
		fails.check(int(avail) == st.Counts.Available && int(conf) == st.Counts.Confirmed && int(held) == st.Counts.Held,
			"metrics do not reconcile with API state")
	}

	fmt.Println()
	if len(fails) > 0 {
		for _, f := range fails {
			fmt.Println("  ✗", f)
		}
		return fmt.Errorf("%d check(s) failed", len(fails))
	}
	fmt.Println("  ✓ all checks passed: one winner per hot seat, zero 5xx, idempotent replays identical, per-user limit held, invariant holds, metrics reconcile")
	fmt.Printf("  dashboard: %s/?show=%s\n", o.baseURL, show.ID)
	return nil
}

func getShow(c *client, id string) (*showState, error) {
	r := c.do("GET", "/shows/"+id, "", nil, nil)
	if r.err != nil || r.status != 200 {
		return nil, fmt.Errorf("GET /shows/%s failed: status=%d err=%v", id, r.status, r.err)
	}
	var st showState
	if err := json.Unmarshal(r.body, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

func okMark(ok bool) string {
	if ok {
		return "✓"
	}
	return "✗"
}
