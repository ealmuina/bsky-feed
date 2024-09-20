CREATE TABLE posts
(
    uri          VARCHAR(255) PRIMARY KEY,

    author_did   VARCHAR(255) REFERENCES "users",

    cid          VARCHAR(255) NOT NULL,
    reply_parent VARCHAR(255) NULL,
    reply_root   VARCHAR(255) NULL,

    indexed_at   TIMESTAMP DEFAULT current_timestamp,
    created_at   TIMESTAMP
);

CREATE INDEX IF NOT EXISTS "idx_posts_uri" ON "posts" ("uri");
CREATE INDEX IF NOT EXISTS "idx_posts_author_did" ON "posts" ("author_did");
CREATE INDEX IF NOT EXISTS "idx_posts_created_at" ON "posts" ("created_at");