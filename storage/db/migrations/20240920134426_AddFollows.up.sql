CREATE TABLE follows
(
    uri         VARCHAR(255) PRIMARY KEY,

    author_did  VARCHAR(255) NOT NULL,
    subject_did VARCHAR(255) NOT NULL,

    indexed_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at  TIMESTAMP    NOT NULL
);

CREATE INDEX idx_follows_author_did ON follows (author_did);
CREATE INDEX idx_follows_subject_did ON follows (subject_did);
