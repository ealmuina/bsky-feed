-- name: CreateFollow :exec
INSERT INTO follows (uri, author_did, subject_did, created_at)
VALUES ($1, $2, $3, $4);

-- name: DeleteFollow :one
DELETE
FROM follows
WHERE uri = $1
RETURNING author_did, subject_did;