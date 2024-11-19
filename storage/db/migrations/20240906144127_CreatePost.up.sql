CREATE TABLE posts
(
    id           SERIAL PRIMARY KEY,
    uri_key      TEXT                                NOT NULL,
    author_id    INT                                 NOT NULL,

    reply_parent TEXT[],
    reply_root   TEXT[],
    language     TEXT,

    indexed_at   TIMESTAMP DEFAULT current_timestamp NOT NULL,
    created_at   TIMESTAMP                           NOT NULL,

    UNIQUE (uri_key, author_id)
);

CREATE INDEX IF NOT EXISTS idx_posts_key ON posts (uri_key, author_id);
CREATE INDEX IF NOT EXISTS idx_posts_author_id ON posts (author_id);
CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts (created_at);
CREATE INDEX IF NOT EXISTS idx_posts_reply_root_is_null ON posts (reply_root) WHERE reply_root IS NULL;
