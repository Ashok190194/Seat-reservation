# WRITEUP

## 1. The atomic decision

**Mechanism: one Postgres transaction per reserve, row locks acquired in a fixed global order, then a state-guarded conditional `UPDATE`.** Nothing is decided in application memory.

Inside `store.Reserve` (`internal/store/store.go`), in this order:

1. `INSERT INTO idempotency_keys … ON CONFLICT DO NOTHING` — see §2. This is the first lock the transaction takes.
2. `SELECT pg_advisory_xact_lock(hashtextextended(show_id || ':' || user_id, 0))` — serialises *one user's* parallel requests on *one show*. Different users never contend on this lock.
3. `SELECT label, status FROM seats WHERE show_id = $1 AND label = ANY($2) ORDER BY label FOR UPDATE` — takes row locks on exactly the requested seats, in label order. Postgres sorts first and locks in output order (the `LockRows` node sits above `Sort`), so two transactions wanting overlapping sets always lock their intersection in the same order.
4. With the rows pinned: if any seat is not `available` → decline `seat_taken` (all-or-nothing). Then `SELECT count(*) FROM seats WHERE show_id=$1 AND user_id=$2`; if `holding + requested > per_user_limit` → decline `per_user_limit`.
5. `INSERT reservations …` then `UPDATE seats SET status=…, reservation_id=…, user_id=… WHERE show_id=$1 AND label=ANY($2) AND status='available'`. The row count must equal the number of requested seats, otherwise the transaction is aborted — the lock makes this impossible, the guard makes it *checked*.
6. Record the outcome on the idempotency row; `COMMIT`.

**Why it is race-free.** A seat is a single row with primary key `(show_id, label)`. For 500 concurrent requests on A12, step 3 queues 499 of them behind the first `FOR UPDATE`. When the winner commits, the next transaction's `SELECT … FOR UPDATE` re-reads the row under READ COMMITTED and sees `status='confirmed'`, so step 4 declines it. There is no window between "read" and "write": the read *is* the lock. The schema adds a second line of defence: `CHECK ((status='available') = (reservation_id IS NULL))` and friends, so a half-written seat row cannot exist.

**Multi-seat requests and deadlock.** Deadlock needs a cycle of waits. Every transaction acquires locks in the same global order — idempotency key → user advisory lock → seats sorted by label → (for cancel/confirm) the reservation row — so a cycle cannot form:

- Seat locks are always taken in sorted order within one statement; `{A12,A13}` vs `{A13,A12}` both lock A12 first.
- A transaction waiting on the user advisory lock holds no seat locks yet; a transaction holding seat locks already holds its user lock.
- `Cancel`/`Confirm` lock the reservation's seats (sorted, `FOR UPDATE`) *before* locking the reservation row, matching the sweeper, which releases seats first and marks reservations afterwards.
- The expiry sweeper uses `FOR UPDATE SKIP LOCKED`, so it never waits on anything and can never be a member of a cycle.

The test `TestSweeperAndReserveDoNotDeadlock` hammers overlapping two-seat reserves against a sweeper doing 7-row batches; `TestMultiSeatOppositeOrderNoDeadlockAllOrNothing` covers the opposite-order case. If a deadlock or serialization failure ever did surface, `withRetry` re-runs the transaction (≤3 attempts) and bumps `db_tx_retries_total`; across every burst run so far that counter has stayed at 0.

**Per-user limit under concurrency.** Without step 2, a user firing 10 parallel single-seat requests would have each transaction lock *its own* seat, count `0` existing, and all 10 would pass. The transaction-scoped advisory lock on `(show, user)` makes those 10 run one at a time (only for that user), so the count in step 4 is exact. Cost: zero for the common case of distinct users.

## 2. Idempotency

**Where the key lives.** Table `idempotency_keys` with primary key `(user_id, key)`, plus `request_hash`, `response_code`, `response_body` (TEXT), `reservation_id`. Keys are scoped per user, so two users can both use `"attempt-1"` without colliding, and a stolen key cannot replay someone else's reservation.

**Exactly-once.** The key row is inserted *inside the same transaction* as the seat update, as its first statement, with `ON CONFLICT DO NOTHING`:

- Row inserted → we are the first; proceed to decide, then `UPDATE` the row with the final status code and body before `COMMIT`. Reservation and key commit atomically — there is no state where the seat is sold but the key is missing, or vice versa.
- Zero rows inserted → someone committed this key already. If a concurrent duplicate is still in flight, Postgres blocks our `INSERT` on the uncommitted unique-index entry until that transaction commits or aborts, so by the time we see "0 rows" the result exists. We read it and replay the stored `response_code`/`response_body` verbatim with an `Idempotent-Replayed: true` header. If the first attempt aborted (crash, 5xx), its row vanished with it and the retry simply becomes the first attempt — which is exactly the semantics a client wants from "retry with the same key".

Declines are stored and replayed too: a client that got `409 seat_taken` and retries the same key gets the same `409`, even if the seat has since been freed. Replaying an error is the standard (Stripe-style) behaviour and keeps the contract simple: *one key, one decision*. The response body is stored as `TEXT`, not `JSONB`, precisely so the replay is byte-identical (the burst tool asserts this; JSONB re-orders keys).

**Same key, different body.** `request_hash = sha256(show_id, sorted seat labels)`. If the stored hash differs from the incoming one → `409 idempotency_key_reused`, without touching anything. Seat order and duplicates are normalised before hashing so `["A1","A2"]` and `["A2","A1"]` count as the same request.

Keys are optional (header `Idempotency-Key` or body `idempotency_key`; both present and different → `400`). The grader's burst may or may not send them; without a key the request is simply independent.

## 3. Holds & expiry

Both models from the brief are implemented; the show chooses.

- `hold_ttl_seconds = 0` (default, matches the brief's sample response): reserve is an immediate sale (`status: confirmed`). Only an explicit, owner-only `POST /reservations/{id}/cancel` frees seats.
- `hold_ttl_seconds > 0`: reserve creates `status: held` with `expires_at`; the seat rows carry the same `hold_expires_at`. The owner converts it with `POST …/confirm`, whose `UPDATE … WHERE reservation_id=$1 AND status='held' AND hold_expires_at > now()` must touch every seat of the reservation or it is declined `hold_expired`. A sweeper goroutine (every `SWEEP_INTERVAL`, default 1 s) runs one statement that selects expired held seats `FOR UPDATE SKIP LOCKED`, flips them to `available`, and marks the owning reservations `expired`. It is idempotent and safe to run from many instances at once.

**No resurrection.** Every release is keyed on `reservation_id`: `UPDATE seats SET status='available' … WHERE reservation_id = $1`. If Alice's hold expired and Bob then bought the seat, the seat row now points at Bob's reservation, so Alice's late cancel matches zero rows. Her reservation row is already `expired`, so the API answers `409 reservation_expired`. `TestHoldExpiryAndNoResurrection` walks exactly this sequence.

Held seats count towards the per-user limit and towards `held` in the reconciliation invariant; the invariant is a plain `GROUP BY status` over seat rows, so it is true by construction at every instant.

## 4. Consistency vs availability under a partition

This service is **CP**. The system of record is one Postgres primary and every decision is a committed transaction there. If the API cannot reach the database:

- `/readyz` returns `503` within 2 s so the platform stops routing to that instance (`/healthz` stays `200` — the process is alive, just not useful).
- In-flight reserves fail with `503 unavailable` and a message telling the client to retry *with the same idempotency key*; because the key commits atomically with the sale, a retry can never double-book.
- Nothing is decided locally, cached, or queued for later. We would rather turn buyers away for the duration of a partition than tell two of them they own A12.

This is the right trade for selling unique inventory: a wrongly-confirmed seat is a refund, a support ticket and a very unhappy customer at the door; a 503 for ten seconds is a retry. Availability is bought with replicas and failover of the database, not by relaxing the invariant. Reads (`GET /shows/{id}`, `/metrics` gauges) could be served from a replica with bounded staleness without affecting correctness, but currently also go to the primary for simplicity.

Horizontal scaling of the API is free — it holds no state, advisory locks are transaction-scoped and `pgbouncer`-safe, and the sweeper is multi-instance safe — so the only scaling limit is the primary's write throughput, which is what one would expect for a single hall's inventory.

## 5. Observability — what wakes me at 2 am

Everything below is already exported; the alert rules are what I would wire into Prometheus/Alertmanager.

| Page | Expression (sketch) | Why |
|---|---|---|
| **Invariant broken** | `sum by(show_id)(seats_by_status) != seats_total` | The only unrecoverable-by-retry bug. Should never fire; if it does, stop sales. |
| **Any 5xx on reserve** | `increase(http_requests_total{route="POST /shows/{id}/reserve",status=~"5.."}[5m]) > 0` | Declines are 4xx; a 5xx means the DB or a bug. |
| **Readiness flapping** | `up == 0` on the readiness probe, or `seats_scrape_error == 1` | DB unreachable. |
| **Deadlock/serialization retries** | `increase(db_tx_retries_total[10m]) > 0` | Lock ordering assumption violated somewhere. Not customer-visible yet, but it will be. |
| **Pool saturation** | `db_pool_acquire_wait_seconds_total` rate climbing, `http_requests_in_flight` high while `reservations_*_total` rates flat | Requests are queueing for connections instead of being served. |
| **Latency** | `histogram_quantile(0.99, reserve_transaction_duration_seconds) > 1s` | Hot-seat lock queues are expected to add latency, but seconds means a long-held lock or a slow DB. |
| **Expiry sweeper stalled** | `held` gauge non-zero while `increase(reservations_expired_total[5m]) == 0` on a show with a TTL | Holds not returning to inventory. |

Things I would *watch* but not page on: `reservations_declined_total{reason="seat_taken"}` vs `reservations_confirmed_total` ratio (demand shape), `idempotent_replay` rate (client retry storms), `per_user_limit` (scalpers).

Logs are JSON with a `request_id` on every line (echoed from `X-Request-ID` or generated, returned in the response header), the matched `route`, status, duration, `user_id` and a domain `outcome` string. A support question "why did buyer-00042 not get A12 at 19:00:01?" is one filter away. The dashboard at `/` polls the show state and the counters once a second so a human can watch a burst land.

## 6. AI usage — directed vs decided

Be honest about this section; it is read before the interview.

The first complete version of this repository was produced by an AI coding agent (Claude, running as a Cursor cloud agent) working from the challenge brief in one session. Concretely, the agent:

- **Decided** the stack (Go + Postgres), the locking design (idempotency row → per-user advisory lock → sorted `FOR UPDATE` → guarded `UPDATE`), the "declines are replayed too" idempotency semantics, the dual hold model with `hold_ttl_seconds`, and the `SKIP LOCKED` sweeper.
- **Wrote** the service, the schema, the burst tool, the tests, the Docker/Render/Fly configs, and these documents.
- **Found and fixed** two bugs by running the code, not by reading it: the sweeper's `RETURNING reservation_id` returning the post-update `NULL` (fixed with a CTE), and JSONB normalising stored responses so replays were not byte-identical (fixed by storing `TEXT`).
- **Verified** against a real Postgres: the burst tool (20 000 requests, 500 concurrent, five hot seats × 500 buyers) and the race tests under `-race`, plus a clean-clone Docker build and a fail-closed readiness check.

What the human directed and what they changed afterwards should be listed here by the candidate before submission — e.g. design choices overridden, code rewritten, parts re-derived by hand. The depth that will be tested live is the reasoning in §1–§4; anyone presenting this repo should be able to re-derive the lock-order argument and the idempotency commit argument at a whiteboard without the code in front of them.

## 7. What I would do next

1. **Deploy and burst the live URL** from a different region, then tune `DB_MAX_CONNS` against the real Postgres plan's connection ceiling; put `pgbouncer` in transaction mode in front if the plan is tight.
2. **Payment step.** Make `confirm` take a payment intent id and make it idempotent on that id; today `confirm` simulates payment success.
3. **Queue the front door.** For true on-sale spikes, a waiting-room token (admit N users/second) in front of `/reserve` keeps DB lock queues short and p99 flat; correctness does not depend on it, latency does.
4. **Read replicas for `GET /shows/{id}`** and the metric gauges, with staleness exposed as a metric.
5. **Key TTL.** Idempotency rows live forever; add `created_at`-based expiry (24 h) via the sweeper.
6. **Seat maps and pricing tiers.** Move `price_paise` from the show to the seat row and sum the locked rows in step 5; today `amount_paise = price_paise × seats`.
7. **Audit log.** An append-only `seat_events` table written in the same transaction as each state change, for support and reconciliation against the payment ledger.
8. **OpenTelemetry traces** with the request id as a span attribute, so a slow reserve can be attributed to lock wait vs. pool wait vs. network.
