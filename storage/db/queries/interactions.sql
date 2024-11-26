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

-- name: VacuumInteractions :exec
VACUUM ANALYSE interactions;
