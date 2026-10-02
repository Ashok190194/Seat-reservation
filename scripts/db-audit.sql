-- Read-only correctness audit. Run it against any deployment:
--   psql "<DATABASE_URL>" -f scripts/db-audit.sql
-- Every check says what it should return; anything else is a bug.
SET default_transaction_read_only = on;
\pset footer off

\echo '== 1. reconciliation invariant per show (expect invariant_ok = t everywhere)'
SELECT s.name, s.total_seats,
       count(*) FILTER (WHERE st.status = 'available') AS available,
       count(*) FILTER (WHERE st.status = 'held')      AS held,
       count(*) FILTER (WHERE st.status = 'confirmed') AS confirmed,
       count(*) = s.total_seats                        AS invariant_ok
FROM shows s JOIN seats st ON st.show_id = s.id
GROUP BY s.id ORDER BY s.created_at DESC;

\echo '== 2. no double-sell: a seat claimed by more than one live reservation (expect 0 rows)'
SELECT r.show_id, seat, count(*) AS live_claims
FROM reservations r CROSS JOIN LATERAL unnest(r.seats) AS seat
WHERE r.status IN ('held', 'confirmed')
GROUP BY r.show_id, seat HAVING count(*) > 1;

\echo '== 3a. seat rows that disagree with the reservation they point at (expect 0 rows)'
SELECT st.show_id, st.label, st.status AS seat_status, r.status AS reservation_status
FROM seats st JOIN reservations r ON r.id = st.reservation_id
WHERE st.status <> r.status OR st.user_id <> r.user_id OR NOT (st.label = ANY (r.seats));

\echo '== 3b. live reservations whose seats do not point back at them (expect 0 rows)'
SELECT r.id, r.status, seat
FROM reservations r CROSS JOIN LATERAL unnest(r.seats) AS seat
LEFT JOIN seats st ON st.show_id = r.show_id AND st.label = seat AND st.reservation_id = r.id
WHERE r.status IN ('held', 'confirmed') AND st.label IS NULL;

\echo '== 4. per-user limit never exceeded (expect 0 rows)'
SELECT st.show_id, st.user_id, count(*) AS seats, s.per_user_limit
FROM seats st JOIN shows s ON s.id = st.show_id
WHERE st.user_id IS NOT NULL
GROUP BY st.show_id, st.user_id, s.per_user_limit
HAVING count(*) > s.per_user_limit;

\echo '== 5. money: amount_paise = price x seats for every reservation (expect 0 rows)'
SELECT r.id, r.amount_paise, s.price_paise, cardinality(r.seats) AS n
FROM reservations r JOIN shows s ON s.id = r.show_id
WHERE r.amount_paise <> s.price_paise * cardinality(r.seats);

\echo '== 6. revenue per show from confirmed reservations'
SELECT s.name, s.price_paise, count(r.*) AS reservations,
       coalesce(sum(cardinality(r.seats)), 0) AS seats_sold, coalesce(sum(r.amount_paise), 0) AS revenue_paise
FROM shows s LEFT JOIN reservations r ON r.show_id = s.id AND r.status = 'confirmed'
GROUP BY s.id ORDER BY s.created_at DESC;

\echo '== 7. idempotency ledger: stored outcomes, and no reservation created under two keys (expect 0 rows)'
SELECT response_code, count(*) AS keys FROM idempotency_keys GROUP BY response_code ORDER BY response_code;
SELECT reservation_id, count(*) FROM idempotency_keys
WHERE reservation_id IS NOT NULL GROUP BY reservation_id HAVING count(*) > 1;

\echo '== 8. the constraints that make bad states unrepresentable'
SELECT conrelid::regclass AS "table", conname, pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE conrelid IN ('seats'::regclass, 'reservations'::regclass, 'idempotency_keys'::regclass, 'shows'::regclass)
  AND contype IN ('p', 'c', 'f')
ORDER BY 1, contype, conname;
