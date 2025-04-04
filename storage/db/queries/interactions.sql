-- name: CreateTempInteractionsTable :exec
CREATE TEMPORARY TABLE tmp_interactions
    ON COMMIT DROP
AS
SELECT *
FROM interactions
    WITH NO DATA;

-- name: BulkCreateInteractions :copyfrom
INSERT INTO tmp_interactions (uri_key, author_id, kind, post_id, created_at)
VALUES ($1, $2, $3, $4, $5);

-- name: CreateInteraction :one
INSERT INTO interactions (uri_key, author_id, kind, post_id, created_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (author_id, post_id, kind) DO UPDATE
    SET uri_key    = CASE
                         WHEN excluded.created_at > interactions.created_at
                             THEN excluded.uri_key
                         ELSE
                             interactions.uri_key
        END,
        created_at = GREATEST(interactions.created_at, excluded.created_at)
RETURNING id, XMAX = 0 AS is_created;

-- name: InsertFromTempToInteractions :many
INSERT INTO interactions (uri_key, author_id, kind, post_id, created_at)
SELECT uri_key, author_id, kind, post_id, created_at
FROM tmp_interactions
ON CONFLICT DO NOTHING
RETURNING id, post_id;

-- name: BulkDeleteInteractions :many
DELETE
FROM interactions
WHERE author_id = ANY (@author_ids::INT[])
  AND uri_key = ANY (@uri_keys::VARCHAR[])
RETURNING id, post_id;

-- name: DeleteInteraction :one
DELETE
FROM interactions
WHERE author_id = $1
  AND uri_key = $2
RETURNING id, post_id;

-- name: GetUserInteractions :many
SELECT *
FROM interactions
WHERE author_id = $1;

-- name: GetPostInteractions :many
SELECT id, uri_key, author_id
FROM interactions
WHERE post_id = $1;

-- name: DeleteOldInteractions :exec
DELETE
FROM interactions
WHERE created_at < current_timestamp - interval '7 days';
