CREATE TABLE posts
(
    uri          VARCHAR PRIMARY KEY,

    author_did   VARCHAR REFERENCES "users",

    cid          VARCHAR UNIQUE NOT NULL,
    reply_parent VARCHAR        NULL,
    reply_root   VARCHAR        NULL,

    indexed_at   TIMESTAMP DEFAULT current_timestamp,
    created_at   TIMESTAMP
);

CREATE INDEX IF NOT EXISTS "idx_posts_uri" ON "posts" ("uri");
CREATE INDEX IF NOT EXISTS "idx_posts_created_at" ON "posts" ("created_at");