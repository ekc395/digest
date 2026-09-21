-- One item belongs to exactly one feed (locked decision 5). The same article
-- arriving from three feeds is three rows, linked later by a cluster.
CREATE TABLE item (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed_id        BIGINT NOT NULL REFERENCES feed (id),
    -- Computed by the fallback chain (locked decision 1). An empty key would
    -- collapse an entire feed into one row, so the CHECK guards against the
    -- chain silently bottoming out.
    dedup_key      TEXT NOT NULL CHECK (dedup_key <> ''),
    -- Bump when the chain changes, or every existing item stops matching its
    -- own future self and the archive fragments permanently.
    dedup_key_ver  INT NOT NULL,
    guid           TEXT,
    url            TEXT,
    canonical_url  TEXT,
    title          TEXT,
    author         TEXT,
    -- Claimed by the feed. Often garbage: 1970, the future, or the feed's own
    -- build date on every item. Display it, never sort by it.
    published_at   TIMESTAMPTZ,
    -- Ours, therefore trustworthy. Sort and window on this (locked decision 2).
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    content_html   TEXT,
    content_hash   BYTEA,
    extras         JSONB,
    UNIQUE (feed_id, dedup_key)
);

CREATE INDEX idx_item_first_seen ON item (first_seen_at DESC);

-- One row per attempt, append-only. The fastest-growing table here and the
-- only way to tell a 403 from an empty feed from a genuinely idle one.
CREATE TABLE fetch_attempt (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed_id       BIGINT NOT NULL REFERENCES feed (id),
    started_at    TIMESTAMPTZ NOT NULL,
    duration_ms   INT,
    -- Must stay in lockstep with the FetchOutcome sealed interface. Adding a
    -- variant in Java without adding it here fails at INSERT, not at compile.
    outcome       TEXT NOT NULL CHECK (outcome IN
                      ('ok','not_modified','http_error','timeout','parse_error','empty')),
    http_status   INT,
    bytes         INT,
    items_seen    INT,
    items_new     INT,
    error_class   TEXT,
    error_detail  TEXT
);

CREATE INDEX idx_fetch_attempt_feed_started ON fetch_attempt (feed_id, started_at DESC);
