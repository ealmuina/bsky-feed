-- name: CreateTempFollowsTable :exec
CREATE TEMPORARY TABLE tmp_follows
    ON COMMIT DROP
AS
SELECT *
FROM follows
    WITH NO DATA;

-- name: BulkCreateFollows :copyfrom
INSERT INTO tmp_follows (uri_key, author_id, subject_id)
VALUES ($1, $2, $3);

-- name: InsertFromTempToFollows :many
INSERT INTO follows (uri_key, author_id, subject_id)
SELECT uri_key, author_id, subject_id
FROM tmp_follows
ON CONFLICT DO NOTHING
RETURNING uri_key, author_id, subject_id;

-- name: BulkDeleteFollows :many
DELETE
FROM follows
WHERE uri_key = ANY (@uri_keys::VARCHAR[])
  AND author_id = ANY (@author_ids::INT[])
RETURNING id, author_id, subject_id;