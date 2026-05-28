CREATE TABLE IF NOT EXISTS snapshots (
    stream_id    TEXT    PRIMARY KEY,
    version      INTEGER NOT NULL,
    type         TEXT    NOT NULL,
    content_type TEXT    NOT NULL,
    recorded_at  INTEGER NOT NULL,
    metadata     TEXT    NOT NULL DEFAULT '{}',
    payload      BLOB    NOT NULL
);
