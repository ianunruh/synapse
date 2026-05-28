CREATE TABLE IF NOT EXISTS events (
    global_position INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT    NOT NULL,
    stream_id       TEXT    NOT NULL,
    version         INTEGER NOT NULL,
    type            TEXT    NOT NULL,
    content_type    TEXT    NOT NULL,
    recorded_at     INTEGER NOT NULL,
    causation       TEXT    NOT NULL DEFAULT '',
    correlation     TEXT    NOT NULL DEFAULT '',
    metadata        TEXT    NOT NULL DEFAULT '{}',
    payload         BLOB    NOT NULL,
    UNIQUE(stream_id, version)
);
