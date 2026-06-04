CREATE TABLE IF NOT EXISTS jobs (
    id          SERIAL PRIMARY KEY,
    status      TEXT        NOT NULL DEFAULT 'pending',
    retry_count INT         NOT NULL DEFAULT 0,
    max_retries INT         NOT NULL DEFAULT 3,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);