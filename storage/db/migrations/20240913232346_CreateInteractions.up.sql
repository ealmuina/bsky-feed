CREATE TABLE interactions
(
    id           BIGSERIAL PRIMARY KEY,
    uri_key        TEXT      NOT NULL,
    author_id      INT       NOT NULL,
    kind           SMALLINT  NOT NULL,

    post_author_id INT       NOT NULL,
    post_uri_key TEXT NOT NULL,

    created_at     TIMESTAMP NOT NULL,

    UNIQUE (author_id, uri_key)
);

CREATE INDEX IF NOT EXISTS idx_interactions_keyt ON interactions (author_id, uri_key);
CREATE INDEX IF NOT EXISTS idx_interactions_created_at ON interactions (created_at);
CREATE INDEX IF NOT EXISTS idx_interactions_post_author_id_post_uri_key ON interactions (post_author_id, post_uri_key);
