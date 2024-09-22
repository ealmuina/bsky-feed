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

-- name: GetUserDidsToRefreshEngagement :many
SELECT DISTINCT u.did
FROM users u
         INNER JOIN posts p ON u.did = p.author_did
WHERE u.followers_count > 300
  AND p.created_at <= current_timestamp - interval '1 day'
  AND p.created_at > current_timestamp - interval '2 days';

-- name: RefreshUserEngagement :one
WITH engagement_data AS (SELECT u1.did,
                                (SELECT COUNT(i.uri)
                                 FROM interactions i
                                          INNER JOIN posts p ON p.uri = i.post_uri
                                 WHERE p.author_did = u1.did
                                   AND i.created_at > now() - interval '7 days'
                                   AND p.created_at < now() - interval '1 day') AS count_interactions,
                                (SELECT COUNT(DISTINCT p.uri)
                                 FROM posts p
                                 WHERE p.author_did = u1.did
                                   AND p.created_at < now() - interval '1 day') AS count_posts
                         FROM users u1
                         WHERE u1.did = $1)
UPDATE users u
SET engagement_factor = ((q.count_interactions / NULLIF(q.count_posts::float, 0)) * 100 / u.followers_count) /
                        (5 / log(NULLIF(u.followers_count, 0)))
FROM engagement_data q
WHERE u.did = q.did
RETURNING u.did, u.followers_count, u.follows_count, u.posts_count, u.engagement_factor;
