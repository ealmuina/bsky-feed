-- name: CreateTempPostsTable :exec
CREATE TEMPORARY TABLE tmp_posts
    ON COMMIT DROP
AS
SELECT *
FROM posts
    WITH NO DATA;

-- name: BulkCreatePosts :copyfrom
INSERT INTO tmp_posts (uri_key, author_id, reply_parent, reply_root, created_at, language)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: InsertFromTempToPosts :many
INSERT INTO posts (uri_key, author_id, reply_parent, reply_root, created_at, language)
SELECT uri_key, author_id, reply_parent, reply_root, created_at, language
FROM tmp_posts
ON CONFLICT DO NOTHING
RETURNING id, author_id, reply_root;

-- name: BulkDeletePosts :many
DELETE
FROM posts
WHERE author_id = ANY (@author_ids::INT[])
  AND uri_key = ANY (@uri_keys::VARCHAR[])
RETURNING id, author_id, uri_key;

-- name: GetOldPosts :many
SELECT id, author_id
FROM posts
WHERE posts.created_at < current_timestamp - interval '7 days';

-- name: GetUserPosts :many
SELECT id, uri_key, author_id
FROM posts
WHERE author_id = $1;
