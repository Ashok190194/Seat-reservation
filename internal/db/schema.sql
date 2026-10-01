-- Idempotent schema: applied on every boot. All money is integer paise.

CREATE TABLE IF NOT EXISTS shows (
    id               UUID PRIMARY KEY,
    name             TEXT        NOT NULL,
    price_paise      BIGINT      NOT NULL CHECK (price_paise >= 0),
    per_user_limit   INT         NOT NULL CHECK (per_user_limit >= 1),
    hold_ttl_seconds INT         NOT NULL CHECK (hold_ttl_seconds >= 0),
    total_seats      INT         NOT NULL CHECK (total_seats >= 1),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per physical seat. The (show_id, label) primary key is what makes a
-- double-hold structurally impossible: there is exactly one row to win, and the
-- reserve transaction takes a row lock on it before deciding.
CREATE TABLE IF NOT EXISTS seats (
    show_id         UUID        NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
    label           TEXT        NOT NULL,
    position        INT         NOT NULL,
    status          TEXT        NOT NULL CHECK (status IN ('available', 'held', 'confirmed')),
    reservation_id  UUID        NULL,
    user_id         TEXT        NULL,
    hold_expires_at TIMESTAMPTZ NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (show_id, label),
    -- A seat that is taken always points at who took it; a free seat never does.
    CHECK ((status = 'available') = (reservation_id IS NULL)),
    CHECK ((status = 'available') = (user_id IS NULL)),
    CHECK (status <> 'held' OR hold_expires_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS seats_by_reservation ON seats (reservation_id) WHERE reservation_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS seats_by_user        ON seats (show_id, user_id) WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS seats_expiring       ON seats (hold_expires_at) WHERE status = 'held';

CREATE TABLE IF NOT EXISTS reservations (
    id           UUID        PRIMARY KEY,
    show_id      UUID        NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
    user_id      TEXT        NOT NULL,
    seats        TEXT[]      NOT NULL,
    amount_paise BIGINT      NOT NULL CHECK (amount_paise >= 0),
    status       TEXT        NOT NULL CHECK (status IN ('held', 'confirmed', 'cancelled', 'expired')),
    expires_at   TIMESTAMPTZ NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS reservations_by_user ON reservations (user_id, created_at DESC);

-- Exactly-once ledger for POST /reserve. The primary key is the atomic step:
-- two concurrent requests with the same key both INSERT; the second blocks on
-- the first's uncommitted row and, once the first commits, sees it and replays.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    user_id        TEXT        NOT NULL,
    key            TEXT        NOT NULL,
    request_hash   TEXT        NOT NULL,
    response_code  INT         NOT NULL,
    response_body  JSONB       NOT NULL,
    reservation_id UUID        NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, key)
);
