-- name: BulkCreateInteractions :copyfrom
INSERT INTO interactions (uri, kind, author_did, post_uri, created_at)
VALUES ($1, $2, $3, $4, $5);

-- name: BulkDeleteInteractions :many
DELETE
FROM interactions
WHERE uri = ANY ($1::VARCHAR[])
RETURNING uri, post_uri;

-- name: DeleteUserInteractions :exec
DELETE
FROM interactions
WHERE author_did = $1;

-- name: DeleteOldInteractions :exec
DELETE
FROM interactions
WHERE created_at < current_timestamp - interval '7 days';

-- name: VacuumInteractions :exec
VACUUM interactions;
