-- name: CreateTempFollowsTable :exec
CREATE TEMPORARY TABLE tmp_follows
    ON COMMIT DROP
AS
SELECT *
FROM follows
    WITH NO DATA;

-- name: BulkCreateFollows :copyfrom
INSERT INTO tmp_follows (uri, author_did, subject_did, created_at)
VALUES ($1, $2, $3, $4);

-- name: InsertFromTempToFollows :many
INSERT INTO follows (uri, author_did, subject_did, created_at)
SELECT uri, author_did, subject_did, created_at
FROM tmp_follows
ON CONFLICT DO NOTHING
RETURNING uri, author_did, subject_did;

-- name: BulkDeleteFollows :many
DELETE
FROM follows
WHERE uri = ANY (@uris::VARCHAR[])
RETURNING author_did, subject_did;