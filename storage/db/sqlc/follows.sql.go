// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: follows.sql

package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

const createFollow = `-- name: CreateFollow :exec
INSERT INTO follows (uri, author_did, subject_did, created_at)
VALUES ($1, $2, $3, $4)
`

type CreateFollowParams struct {
	Uri        string
	AuthorDid  string
	SubjectDid string
	CreatedAt  pgtype.Timestamp
}

func (q *Queries) CreateFollow(ctx context.Context, arg CreateFollowParams) error {
	_, err := q.db.Exec(ctx, createFollow,
		arg.Uri,
		arg.AuthorDid,
		arg.SubjectDid,
		arg.CreatedAt,
	)
	return err
}

const deleteFollow = `-- name: DeleteFollow :one
DELETE
FROM follows
WHERE uri = $1
RETURNING author_did, subject_did
`

type DeleteFollowRow struct {
	AuthorDid  string
	SubjectDid string
}

func (q *Queries) DeleteFollow(ctx context.Context, uri string) (DeleteFollowRow, error) {
	row := q.db.QueryRow(ctx, deleteFollow, uri)
	var i DeleteFollowRow
	err := row.Scan(&i.AuthorDid, &i.SubjectDid)
	return i, err
}
