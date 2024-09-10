CREATE TABLE post_languages
(
    id          SERIAL PRIMARY KEY,
    post_uri VARCHAR(255) REFERENCES "posts" ON DELETE CASCADE,
    language_id INTEGER REFERENCES "languages" ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS "idx_post_languages_id" ON "post_languages" ("id");
CREATE INDEX IF NOT EXISTS "idx_post_languages_post_uri" ON "post_languages" ("post_uri");
CREATE INDEX IF NOT EXISTS "idx_post_languages_language_id" ON "post_languages" ("language_id");