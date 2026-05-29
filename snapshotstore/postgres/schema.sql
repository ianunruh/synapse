CREATE TABLE IF NOT EXISTS snapshots (
    stream_id    TEXT        PRIMARY KEY,
    version      BIGINT      NOT NULL,
    type         TEXT        NOT NULL,
    content_type TEXT        NOT NULL,
    recorded_at  TIMESTAMPTZ NOT NULL,
    metadata     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    payload      BYTEA       NOT NULL
);
