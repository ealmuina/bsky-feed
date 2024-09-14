CREATE TYPE interaction_type AS ENUM ('like', 'repost');

CREATE TABLE interactions
(
    uri        VARCHAR(255)     NOT NULL PRIMARY KEY,
    cid        VARCHAR(255)     NOT NULL,
    kind       interaction_type NOT NULL,

    author_did VARCHAR(255)     NOT NULL,
    post_uri   VARCHAR(255)     NOT NULL,

    indexed_at TIMESTAMP DEFAULT current_timestamp,
    created_at TIMESTAMP
);

CREATE INDEX idx_interactions_kind ON interactions (kind);
CREATE INDEX idx_interactions_author_did ON interactions (author_did);
CREATE INDEX idx_interactions_post_uri ON interactions (post_uri);
CREATE INDEX idx_interactions_created_at ON interactions (created_at);
