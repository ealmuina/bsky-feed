CREATE TABLE follows
(
    id         SERIAL PRIMARY KEY,
    uri_key    TEXT      NOT NULL,
    author_id  INT       NOT NULL,

    subject_id INT       NOT NULL,

    created_at TIMESTAMP NOT NULL,

    UNIQUE (author_id, uri_key)
);

CREATE INDEX IF NOT EXISTS idx_follows_key ON follows (author_id, uri_key);
CREATE INDEX IF NOT EXISTS idx_follows_created_at ON follows (created_at);
CREATE INDEX IF NOT EXISTS idx_follows_subject_id ON follows (subject_id);