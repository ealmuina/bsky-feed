CREATE TABLE posts
(
    id         BIGSERIAL PRIMARY KEY,
    uri_key    TEXT      NOT NULL,
    author_id  INT       NOT NULL,

    reply_parent TEXT[],
    reply_root   TEXT[],
    language     TEXT,

    created_at TIMESTAMP NOT NULL,

    UNIQUE (author_id, uri_key)
);

CREATE INDEX IF NOT EXISTS idx_posts_key ON posts (author_id, uri_key);
CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts (created_at);
CREATE INDEX IF NOT EXISTS idx_posts_reply_root_is_null ON posts (reply_root) WHERE reply_root IS NULL;
