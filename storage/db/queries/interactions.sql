-- name: BulkCreateInteractions :copyfrom
INSERT INTO interactions (uri, cid, kind, author_did, post_uri, created_at)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: BulkDeleteInteractions :exec
DELETE
FROM interactions
WHERE uri = ANY ($1::VARCHAR[]);

-- name: DeleteUserInteractions :exec
DELETE
FROM interactions
WHERE author_did = $1;

-- name: DeleteOldInteractions :exec
DELETE
FROM interactions
WHERE indexed_at < current_timestamp - interval '10 days';