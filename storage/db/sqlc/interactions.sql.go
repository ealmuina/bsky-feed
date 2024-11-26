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
	Kind         int16
	PostUriKey   string
	PostAuthorID int32
	CreatedAt    pgtype.Timestamp
}

const bulkDeleteInteractions = `-- name: BulkDeleteInteractions :many
DELETE
FROM interactions
WHERE author_id = ANY ($1::INT[])
  AND uri_key = ANY ($2::VARCHAR[])
RETURNING id, post_uri_key, post_author_id
`

type BulkDeleteInteractionsParams struct {
	AuthorIds []int32
	UriKeys   []string
}

type BulkDeleteInteractionsRow struct {
	ID           int64
	PostUriKey   string
	PostAuthorID int32
}

func (q *Queries) BulkDeleteInteractions(ctx context.Context, arg BulkDeleteInteractionsParams) ([]BulkDeleteInteractionsRow, error) {
	rows, err := q.db.Query(ctx, bulkDeleteInteractions, arg.AuthorIds, arg.UriKeys)
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
SELECT id, uri_key, author_id, kind, post_author_id, post_uri_key, created_at
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

const getPostInteractions = `-- name: GetPostInteractions :many
SELECT id, uri_key, author_id
FROM interactions
WHERE post_author_id = $1
  AND post_uri_key = $2
`

type GetPostInteractionsParams struct {
	PostAuthorID int32
	PostUriKey   string
}

type GetPostInteractionsRow struct {
	ID       int64
	UriKey   string
	AuthorID int32
}

func (q *Queries) GetPostInteractions(ctx context.Context, arg GetPostInteractionsParams) ([]GetPostInteractionsRow, error) {
	rows, err := q.db.Query(ctx, getPostInteractions, arg.PostAuthorID, arg.PostUriKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetPostInteractionsRow
	for rows.Next() {
		var i GetPostInteractionsRow
		if err := rows.Scan(&i.ID, &i.UriKey, &i.AuthorID); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getUserInteractions = `-- name: GetUserInteractions :many
SELECT id, uri_key, author_id, kind, post_author_id, post_uri_key, created_at
FROM interactions
WHERE author_id = $1
`

func (q *Queries) GetUserInteractions(ctx context.Context, authorID int32) ([]Interaction, error) {
	rows, err := q.db.Query(ctx, getUserInteractions, authorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Interaction
	for rows.Next() {
		var i Interaction
		if err := rows.Scan(
			&i.ID,
			&i.UriKey,
			&i.AuthorID,
			&i.Kind,
			&i.PostAuthorID,
			&i.PostUriKey,
			&i.CreatedAt,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const insertFromTempToInteractions = `-- name: InsertFromTempToInteractions :many
INSERT INTO interactions (uri_key, author_id, kind, post_uri_key, post_author_id, created_at)
SELECT uri_key, author_id, kind, post_uri_key, post_author_id, created_at
FROM tmp_interactions
ON CONFLICT DO NOTHING
RETURNING id, post_uri_key, post_author_id
`

type InsertFromTempToInteractionsRow struct {
	ID           int64
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
