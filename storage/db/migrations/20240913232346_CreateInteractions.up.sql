CREATE TABLE interactions
(
    id         BIGSERIAL PRIMARY KEY,
    uri_key    TEXT      NOT NULL,
    author_id INT    NOT NULL REFERENCES users ON DELETE CASCADE,

    kind       SMALLINT  NOT NULL,
    post_id   BIGINT NOT NULL REFERENCES posts ON DELETE CASCADE,

    created_at TIMESTAMP NOT NULL,

    UNIQUE (author_id, uri_key),
    UNIQUE (author_id, post_id, kind)
);

CREATE INDEX IF NOT EXISTS idx_interactions_created_at ON interactions (created_at);
CREATE INDEX IF NOT EXISTS idx_interactions_post_id ON interactions (post_id);
