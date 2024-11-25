-- name: CreateUser :exec
INSERT INTO users (did, handle, followers_count, follows_count, posts_count, last_update)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT DO NOTHING;

-- name: UpdateUser :exec
UPDATE users
SET handle            = $2,
    followers_count   = $3,
    follows_count     = $4,
    posts_count       = $5,
    last_update       = $6,
    refresh_frequency = greatest(1, 30 - (5 * log($3 + 1)))
WHERE did = $1;

-- name: DeleteUser :exec
DELETE
FROM users
WHERE id = $1;

-- name: AddUserFollowers :exec
UPDATE users
SET followers_count = followers_count + $2
WHERE id = $1;

-- name: AddUserFollows :exec
UPDATE users
SET follows_count = follows_count + $2
WHERE id = $1;

-- name: AddUserPosts :exec
UPDATE users
SET posts_count = users.posts_count + $2
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
   OR last_update < current_timestamp - (refresh_frequency || ' days')::interval;

-- name: SetUserFollow :exec
UPDATE users
SET follows = follows || ('{"' || @rkey::text || '":' || @subject_id::int || '}')::jsonb
WHERE id = @id;

-- name: RemoveUserFollow :one
WITH deleted_key AS (SELECT u.id,
                            u.follows -> @rkey::text AS deleted_value,
                            u.follows - @rkey::text  AS updated_follows
                     FROM users u
                     WHERE u.id = @id)
UPDATE users
SET follows = deleted_key.updated_follows
FROM deleted_key
WHERE users.id = deleted_key.id
RETURNING deleted_key.deleted_value;
