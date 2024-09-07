CREATE TABLE post_languages
(
    id          SERIAL PRIMARY KEY,
    post_uri    VARCHAR REFERENCES "posts",
    language_id INTEGER REFERENCES "languages"
);

CREATE INDEX IF NOT EXISTS "idx_post_languages_id" ON "post_languages" ("id");
CREATE INDEX IF NOT EXISTS "idx_post_languages_language_id" ON "post_languages" ("language_id");