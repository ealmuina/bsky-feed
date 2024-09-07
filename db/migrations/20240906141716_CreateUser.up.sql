CREATE TABLE users
(
    did             VARCHAR PRIMARY KEY,
    handle          VARCHAR   NULL,

    followers_count INTEGER   NULL,
    follows_count   INTEGER   NULL,
    posts_count     INTEGER   NULL,

    indexed_at      TIMESTAMP DEFAULT current_timestamp,
    last_update     TIMESTAMP NULL
);

CREATE INDEX IF NOT EXISTS "idx_users_did" ON "users" ("did");
CREATE INDEX IF NOT EXISTS "idx_users_last_update" ON "users" ("last_update");
CREATE INDEX IF NOT EXISTS "idx_users_followers_count" ON "users" ("followers_count");