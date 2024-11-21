CREATE TABLE follows
(
    id         SERIAL PRIMARY KEY,
    uri_key    TEXT                                NOT NULL,
    author_id  INT                                 NOT NULL,
    subject_id INT                                 NOT NULL,

    UNIQUE (uri_key, author_id)
);

CREATE INDEX IF NOT EXISTS idx_follows_author_id ON follows (author_id);
CREATE INDEX IF NOT EXISTS idx_follows_subject_id ON follows (subject_id);
