-- name: GetUser :one
SELECT *
FROM users
WHERE did = $1
LIMIT 1;

-- name: CreateUser :exec
INSERT INTO users (did, handle, followers_count, follows_count, posts_count, last_update)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT DO NOTHING;

-- name: GetUserDids :many
SELECT users.did
FROM users;

-- name: UpdateUser :exec
UPDATE users
SET handle          = $2,
    followers_count = $3,
    follows_count   = $4,
    posts_count     = $5,
    last_update     = $6
WHERE did = $1;