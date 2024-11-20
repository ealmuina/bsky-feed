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
	UriKey       string
	AuthorID     int32
	Kind         InteractionType
	PostUriKey   string
	PostAuthorID int32
	CreatedAt    pgtype.Timestamp
}

const bulkDeleteInteractions = `-- name: BulkDeleteInteractions :many
DELETE
FROM interactions
WHERE uri_key = ANY ($1::VARCHAR[])
  AND author_id = ANY ($2::INT[])
RETURNING id, post_uri_key, post_author_id
`

type BulkDeleteInteractionsParams struct {
	UriKeys   []string
	AuthorIds []int32
}

type BulkDeleteInteractionsRow struct {
	ID           int32
	PostUriKey   string
	PostAuthorID int32
}

func (q *Queries) BulkDeleteInteractions(ctx context.Context, arg BulkDeleteInteractionsParams) ([]BulkDeleteInteractionsRow, error) {
	rows, err := q.db.Query(ctx, bulkDeleteInteractions, arg.UriKeys, arg.AuthorIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []BulkDeleteInteractionsRow
	for rows.Next() {
		var i BulkDeleteInteractionsRow
		if err := rows.Scan(&i.ID, &i.PostUriKey, &i.PostAuthorID); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const createTempInteractionsTable = `-- name: CreateTempInteractionsTable :exec
CREATE TEMPORARY TABLE tmp_interactions
    ON COMMIT DROP
AS
SELECT id, uri_key, author_id, kind, post_uri_key, post_author_id, indexed_at, created_at
FROM interactions
    WITH NO DATA
`

func (q *Queries) CreateTempInteractionsTable(ctx context.Context) error {
	_, err := q.db.Exec(ctx, createTempInteractionsTable)
	return err
}

const deleteOldInteractions = `-- name: DeleteOldInteractions :exec
DELETE
FROM interactions
WHERE created_at < current_timestamp - interval '7 days'
`

func (q *Queries) DeleteOldInteractions(ctx context.Context) error {
	_, err := q.db.Exec(ctx, deleteOldInteractions)
	return err
}

const deleteUserInteractions = `-- name: DeleteUserInteractions :exec
DELETE
FROM interactions
WHERE author_id = $1
`

func (q *Queries) DeleteUserInteractions(ctx context.Context, authorID int32) error {
	_, err := q.db.Exec(ctx, deleteUserInteractions, authorID)
	return err
}

const insertFromTempToInteractions = `-- name: InsertFromTempToInteractions :many
INSERT INTO interactions (uri_key, author_id, kind, post_uri_key, post_author_id, created_at)
SELECT uri_key, author_id, kind, post_uri_key, post_author_id, created_at
FROM tmp_interactions
ON CONFLICT DO NOTHING
RETURNING id, post_uri_key, post_author_id
`

type InsertFromTempToInteractionsRow struct {
	ID           int32
	PostUriKey   string
	PostAuthorID int32
}

func (q *Queries) InsertFromTempToInteractions(ctx context.Context) ([]InsertFromTempToInteractionsRow, error) {
	rows, err := q.db.Query(ctx, insertFromTempToInteractions)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []InsertFromTempToInteractionsRow
	for rows.Next() {
		var i InsertFromTempToInteractionsRow
		if err := rows.Scan(&i.ID, &i.PostUriKey, &i.PostAuthorID); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const vacuumInteractions = `-- name: VacuumInteractions :exec
VACUUM ANALYSE interactions
`

func (q *Queries) VacuumInteractions(ctx context.Context) error {
	_, err := q.db.Exec(ctx, vacuumInteractions)
	return err
}