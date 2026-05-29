CREATE TABLE IF NOT EXISTS events (
    global_position BIGSERIAL PRIMARY KEY,
    event_id        TEXT        NOT NULL,
    stream_id       TEXT        NOT NULL,
    version         BIGINT      NOT NULL,
    type            TEXT        NOT NULL,
    content_type    TEXT        NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL,
    causation       TEXT        NOT NULL DEFAULT '',
    correlation     TEXT        NOT NULL DEFAULT '',
    metadata        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    payload         BYTEA       NOT NULL,
    UNIQUE (stream_id, version)
);
