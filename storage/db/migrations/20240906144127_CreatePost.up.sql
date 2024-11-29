CREATE TABLE posts
(
    id              BIGSERIAL PRIMARY KEY,
    uri_key         TEXT NOT NULL,
    author_id INT NOT NULL REFERENCES users ON DELETE CASCADE,

    reply_parent_id BIGINT,
    reply_root_id   BIGINT,
    language        TEXT,

    created_at      TIMESTAMP,

    UNIQUE (author_id, uri_key)

);

CREATE INDEX IF NOT EXISTS idx_posts_key ON posts (author_id, uri_key);
CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts (created_at);
CREATE INDEX IF NOT EXISTS idx_posts_reply_root_is_null ON posts (reply_root_id) WHERE reply_root_id IS NULL;
