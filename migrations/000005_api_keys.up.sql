CREATE TABLE api_keys (
    id         BIGSERIAL    PRIMARY KEY,
    name       TEXT         NOT NULL,
    key_hash   TEXT         NOT NULL UNIQUE,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
