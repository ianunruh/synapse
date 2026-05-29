CREATE TABLE IF NOT EXISTS checkpoints (
    name     TEXT   PRIMARY KEY,
    position BIGINT NOT NULL
);
