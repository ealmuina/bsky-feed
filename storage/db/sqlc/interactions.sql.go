// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: interactions.sql

package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

type BulkCreateInteractionsParams struct {
	Uri       string
	Kind      InteractionType
	AuthorDid string
	PostUri   string
	CreatedAt pgtype.Timestamp
}

const bulkDeleteInteractions = `-- name: BulkDeleteInteractions :exec
DELETE
FROM interactions
WHERE uri = ANY ($1::VARCHAR[])
`

func (q *Queries) BulkDeleteInteractions(ctx context.Context, dollar_1 []string) error {
	_, err := q.db.Exec(ctx, bulkDeleteInteractions, dollar_1)
	return err
}

const deleteOldInteractions = `-- name: DeleteOldInteractions :exec
DELETE
FROM interactions
WHERE indexed_at < current_timestamp - interval '10 days'
`

func (q *Queries) DeleteOldInteractions(ctx context.Context) error {
	_, err := q.db.Exec(ctx, deleteOldInteractions)
	return err
}

const deleteUserInteractions = `-- name: DeleteUserInteractions :exec
DELETE
FROM interactions
WHERE author_did = $1
`

func (q *Queries) DeleteUserInteractions(ctx context.Context, authorDid string) error {
	_, err := q.db.Exec(ctx, deleteUserInteractions, authorDid)
	return err
}