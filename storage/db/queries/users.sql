-- name: CreateUser :exec
INSERT INTO users (did, handle, followers_count, follows_count, posts_count, last_update)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT DO NOTHING;

-- name: UpdateUser :exec
UPDATE users
SET handle          = $2,
    followers_count = $3,
    follows_count   = $4,
    posts_count     = $5,
    last_update     = $6
WHERE did = $1;

-- name: DeleteUser :exec
DELETE
FROM users
WHERE did = $1;

-- name: AddUserFollowers :exec
UPDATE users
SET followers_count = followers_count + $2
WHERE did = $1;

-- name: AddUserFollows :exec
UPDATE users
SET follows_count = follows_count + $2
WHERE did = $1;

-- name: AddUserPosts :exec
UPDATE users
SET posts_count = users.posts_count + $2
WHERE did = $1;

-- name: GetUser :one
SELECT *
FROM users
WHERE did = $1
LIMIT 1;

-- name: GetUserDids :many
SELECT users.did
FROM users;

-- name: GetUserDidsToRefreshStatistics :many
SELECT users.did
FROM users
WHERE last_update IS NULL
   OR last_update < current_timestamp - interval '30 days';

-- name: GetUsersFollows :many
SELECT did, followers_count, follows_count
FROM users;
