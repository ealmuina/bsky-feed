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
    engagement_factor = $6,
    last_update       = $7
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

-- name: CalculateUserEngagement :one
SELECT (
           ((count(i.uri) / count(DISTINCT i.post_uri)::float) * 100 / u.followers_count) / (5 / log(u.followers_count))
           )::float
FROM interactions i
         INNER JOIN posts p ON i.post_uri = p.uri
         INNER JOIN users u ON u.did = p.author_did
WHERE p.author_did = $1
  AND p.reply_root IS NULL
  AND p.created_at < now() - interval '1 day'
  AND i.created_at > now() - interval '7 days'
GROUP BY u.followers_count;
