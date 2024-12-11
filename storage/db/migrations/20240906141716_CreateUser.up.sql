CREATE TABLE users
(
    id                SERIAL PRIMARY KEY,
    did               TEXT NOT NULL UNIQUE,
    handle            TEXT,

    followers_count   INT,
    follows_count     INT,
    posts_count       INT,

    created_at        TIMESTAMP,
    last_update       TIMESTAMP,
    refresh_frequency INT
);

CREATE INDEX IF NOT EXISTS idx_users_created_at ON users (created_at);
CREATE INDEX IF NOT EXISTS idx_users_last_update ON users (last_update);
