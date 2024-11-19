CREATE TYPE interaction_type AS ENUM ('like', 'repost');

CREATE TABLE interactions
(
    id             SERIAL PRIMARY KEY,
    uri_key        TEXT                                NOT NULL,
    author_id      INT                                 NOT NULL,
    kind           interaction_type                    NOT NULL,

    post_uri_key   TEXT                                NOT NULL,
    post_author_id INT                                 NOT NULL,

    indexed_at     TIMESTAMP DEFAULT current_timestamp NOT NULL,
    created_at     TIMESTAMP                           NOT NULL,

    UNIQUE (uri_key, author_id)
);

CREATE INDEX IF NOT EXISTS idx_interactions_key ON interactions (uri_key, author_id);
CREATE INDEX idx_interactions_created_at ON interactions (created_at);
