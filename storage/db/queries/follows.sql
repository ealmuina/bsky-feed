-- name: CreateTempFollowsTable :exec
CREATE TEMPORARY TABLE tmp_follows
    ON COMMIT DROP
AS
SELECT *
FROM follows
    WITH NO DATA;

-- name: BulkCreateFollows :copyfrom
INSERT INTO tmp_follows (uri_key, author_id, subject_id, created_at)
VALUES ($1, $2, $3, $4);

-- name: CreateFollow :one
INSERT INTO follows (uri_key, author_id, subject_id, created_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT DO NOTHING
RETURNING id;

-- name: InsertFromTempToFollows :many
INSERT INTO follows (uri_key, author_id, subject_id, created_at)
SELECT uri_key, author_id, subject_id, created_at
FROM tmp_follows
ON CONFLICT DO NOTHING
RETURNING id, author_id, subject_id;

-- name: BulkDeleteFollows :many
DELETE
FROM follows
WHERE author_id = ANY (@author_ids::INT[])
  AND uri_key = ANY (@uri_keys::VARCHAR[])
RETURNING id, author_id, subject_id;

-- name: DeleteFollow :one
DELETE
FROM follows
WHERE author_id = $1
  AND uri_key = $2
RETURNING id, author_id, subject_id;

-- name: GetFollowsTouchingUser :many
SELECT id, uri_key, author_id
FROM follows
WHERE author_id = $1
   OR subject_id = $1;
