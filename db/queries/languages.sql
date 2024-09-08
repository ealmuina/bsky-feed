-- name: GetLanguage :one
SELECT id
FROM languages
WHERE code = $1;

-- name: CreateLanguage :one
INSERT INTO languages (code)
VALUES ($1)
RETURNING id;
