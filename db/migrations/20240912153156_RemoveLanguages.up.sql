ALTER TABLE posts
    ADD COLUMN language VARCHAR(2) NULL;

CREATE INDEX idx_posts_language ON posts (language);

UPDATE posts
SET language = (SELECT l.code::VARCHAR(2)
                FROM post_languages pl
                         JOIN languages l ON pl.language_id = l.id
                WHERE pl.post_uri = posts.uri
                LIMIT 1);

DROP TABLE post_languages;
DROP TABLE languages;

DROP INDEX idx_posts_cid;
DROP INDEX idx_posts_created_at;
DROP INDEX idx_posts_uri;

CREATE INDEX idx_posts_cid ON posts (cid DESC);
CREATE INDEX idx_posts_created_at ON posts (created_at DESC);
