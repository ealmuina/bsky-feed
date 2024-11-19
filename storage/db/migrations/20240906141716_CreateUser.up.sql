CREATE TABLE users
(
    id              SERIAL PRIMARY KEY,
    did             TEXT                                NOT NULL,
    handle          VARCHAR(255),

    followers_count INTEGER,
    follows_count   INTEGER,
    posts_count     INTEGER,

    indexed_at      TIMESTAMP DEFAULT current_timestamp NOT NULL,
    last_update     TIMESTAMP
);

CREATE INDEX IF NOT EXISTS "idx_users_did" ON "users" ("did");
CREATE INDEX IF NOT EXISTS "idx_users_last_update" ON "users" ("last_update");
