DROP INDEX idx_posts_cid;
DROP INDEX idx_posts_created_at;

CREATE INDEX idx_posts_cid ON posts (cid);
CREATE INDEX idx_posts_created_at ON posts (created_at);
CREATE INDEX idx_posts_uri ON posts (uri);

CREATE TABLE languages
(
    id   SERIAL PRIMARY KEY,
    code VARCHAR(15) UNIQUE NOT NULL
);

CREATE INDEX idx_languages_code ON languages (code);
CREATE INDEX idx_languages_id ON languages (id);

CREATE TABLE post_languages
(
    id          SERIAL PRIMARY KEY,
    post_uri    VARCHAR(255) REFERENCES posts ON DELETE CASCADE,
    language_id INTEGER REFERENCES languages ON DELETE CASCADE
);

CREATE INDEX idx_post_languages_id ON post_languages (id);
CREATE INDEX idx_post_languages_language_id ON post_languages (language_id);
CREATE INDEX idx_post_languages_post_uri ON post_languages (post_uri);

INSERT INTO languages (code)
SELECT DISTINCT language
FROM posts;

INSERT INTO post_languages (post_uri, language_id)
SELECT posts.uri, languages.id
FROM posts
         JOIN languages ON languages.code = posts.language;

ALTER TABLE posts
    DROP COLUMN language;

DROP INDEX idx_posts_language;