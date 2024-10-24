-- name: BulkCreatePosts :copyfrom
INSERT INTO posts (uri, author_did, reply_parent, reply_root, created_at, language, rank)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: BulkDeletePosts :many
DELETE
FROM posts
WHERE uri = ANY (@uris::VARCHAR[])
RETURNING uri, author_did;

-- name: DeleteOldPosts :many
DELETE
FROM posts
WHERE posts.created_at < current_timestamp - interval '7 days'
RETURNING uri, author_did;

-- name: VacuumPosts :exec
VACUUM posts;

-- name: DeleteUserPosts :exec
DELETE
FROM posts
WHERE author_did = $1;

-- name: GetLanguagePosts :many
SELECT posts.*
FROM posts
WHERE language = $1
  AND reply_root IS NULL
  AND rank < $2
ORDER BY rank DESC
LIMIT $3;

-- name: GetLanguageTopPosts :many
SELECT uri as uri,
       ''  as repost_uri,
       created_at,
       rank
FROM posts
         INNER JOIN users u ON posts.author_did = u.did
WHERE language = $1
  AND reply_root IS NULL
  AND u.followers_count > 1000
  AND rank < $2
ORDER BY rank DESC
LIMIT $3;
