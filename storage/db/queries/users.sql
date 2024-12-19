-- name: CreateUser :exec
INSERT INTO users (did, handle, followers_count, follows_count, posts_count, last_update)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT DO NOTHING;

-- name: UpdateUser :exec
UPDATE users
SET handle            = $2,
    created_at        = $3,
    followers_count   = $4,
    follows_count     = $5,
    posts_count       = $6,
    last_update       = $7,
    refresh_frequency = greatest(1, 30 - (5 * log($4 + 1)))
WHERE did = $1;

-- name: DeleteUser :exec
DELETE
FROM users
WHERE id = $1;

-- name: DeleteUserByDid :exec
DELETE
FROM users
WHERE did = $1;

-- name: AddUserFollowers :exec
UPDATE users
SET followers_count = COALESCE(followers_count, 0) + $2
WHERE id = $1;

-- name: AddUserFollows :exec
UPDATE users
SET follows_count = COALESCE(follows_count, 0) + $2
WHERE id = $1;

-- name: GetUser :one
SELECT *
FROM users
WHERE id = $1
LIMIT 1;

-- name: GetUserId :one
SELECT id
FROM users
WHERE did = $1
LIMIT 1;

-- name: GetUserDidsToRefreshStatistics :many
SELECT users.did
FROM users
WHERE last_update IS NULL
   OR last_update < current_timestamp - (COALESCE(refresh_frequency, 30) || ' days')::interval;

-- name: SetUserMetadata :exec
UPDATE users
SET handle     = $2,
    created_at = LEAST(created_at, $3)
WHERE did = $1;
