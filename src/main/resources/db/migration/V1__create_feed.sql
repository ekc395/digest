CREATE TABLE feed (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    site_url TEXT,
    title TEXT,
    kind TEXT NOT NULL CHECK (kind IN ('rss','atom','youtube','reddit','github','arxiv')),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    fetch_interval_s INT NOT NULL DEFAULT 3600,
    next_fetch_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_success_at TIMESTAMPTZ,
    consecutive_failures INT NOT NULL DEFAULT 0,
    etag TEXT,
    last_modified TEXT,
    body_hash BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_feed_next_fetch ON feed (next_fetch_at)
WHERE active;