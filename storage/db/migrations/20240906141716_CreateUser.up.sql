CREATE TABLE users
(
    id                SERIAL PRIMARY KEY,
    did               TEXT NOT NULL,
    handle            VARCHAR(255),

    followers_count   INT,
    follows_count     INT,
    posts_count       INT,

    last_update       TIMESTAMP,
    refresh_frequency INT  NOT NULL DEFAULT 30
);

CREATE INDEX IF NOT EXISTS "idx_users_did" ON "users" ("did");
CREATE INDEX IF NOT EXISTS "idx_users_last_update" ON "users" ("last_update");
