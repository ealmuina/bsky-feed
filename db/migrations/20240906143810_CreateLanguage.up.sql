CREATE TABLE languages
(
    id   SERIAL PRIMARY KEY,
    code VARCHAR UNIQUE NOT NULL
);

CREATE INDEX IF NOT EXISTS "idx_languages_id" ON "languages" ("id");
CREATE INDEX IF NOT EXISTS "idx_languages_code" ON "languages" ("code");
