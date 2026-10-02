-- Watch the database while a burst runs (Ctrl+C to stop):
--   psql "<DATABASE_URL>" -f scripts/db-locks.sql
-- During a hot-seat storm most pool connections wait on 'tuple' or
-- 'transactionid' locks: the losers queued behind the winner's row lock.
-- During the general stampede, 'idle in transaction' dominates: the database
-- is waiting for the application to send its next statement.
SET default_transaction_read_only = on;
SELECT clock_timestamp()::time(1) AS t,
       count(*) FILTER (WHERE wait_event_type = 'Lock')      AS waiting_on_lock,
       count(*) FILTER (WHERE state = 'active')              AS active,
       count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_tx,
       coalesce(string_agg(DISTINCT wait_event, ',') FILTER (WHERE wait_event_type = 'Lock'), '-') AS lock_kinds,
       count(*)                                              AS connections
FROM pg_stat_activity
WHERE datname = current_database() AND backend_type = 'client backend' AND pid <> pg_backend_pid()
\watch 0.5
