// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: follows.sql

package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

type BulkCreateFollowsParams struct {
	UriKey    string
	AuthorID  int32
	SubjectID int32
	CreatedAt pgtype.Timestamp
}

const bulkDeleteFollows = `-- name: BulkDeleteFollows :many
DELETE
FROM follows
WHERE author_id = ANY ($1::INT[])
  AND uri_key = ANY ($2::VARCHAR[])
RETURNING id, author_id, subject_id
`

type BulkDeleteFollowsParams struct {
	AuthorIds []int32
	UriKeys   []string
}

type BulkDeleteFollowsRow struct {
	ID        int64
	AuthorID  int32
	SubjectID int32
}

func (q *Queries) BulkDeleteFollows(ctx context.Context, arg BulkDeleteFollowsParams) ([]BulkDeleteFollowsRow, error) {
	rows, err := q.db.Query(ctx, bulkDeleteFollows, arg.AuthorIds, arg.UriKeys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []BulkDeleteFollowsRow
	for rows.Next() {
		var i BulkDeleteFollowsRow
		if err := rows.Scan(&i.ID, &i.AuthorID, &i.SubjectID); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const createTempFollowsTable = `-- name: CreateTempFollowsTable :exec
CREATE TEMPORARY TABLE tmp_follows
    ON COMMIT DROP
AS
SELECT id, uri_key, author_id, subject_id, created_at
FROM follows
    WITH NO DATA
`

func (q *Queries) CreateTempFollowsTable(ctx context.Context) error {
	_, err := q.db.Exec(ctx, createTempFollowsTable)
	return err
}

const getFollowsTouchingUser = `-- name: GetFollowsTouchingUser :many
SELECT id, uri_key, author_id
FROM follows
WHERE author_id = $1
   OR subject_id = $1
`

type GetFollowsTouchingUserRow struct {
	ID       int64
	UriKey   string
	AuthorID int32
}

func (q *Queries) GetFollowsTouchingUser(ctx context.Context, authorID int32) ([]GetFollowsTouchingUserRow, error) {
	rows, err := q.db.Query(ctx, getFollowsTouchingUser, authorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetFollowsTouchingUserRow
	for rows.Next() {
		var i GetFollowsTouchingUserRow
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

const insertFromTempToFollows = `-- name: InsertFromTempToFollows :many
INSERT INTO follows (uri_key, author_id, subject_id, created_at)
SELECT uri_key, author_id, subject_id, created_at
FROM tmp_follows
ON CONFLICT DO NOTHING
RETURNING id, author_id, subject_id
`

type InsertFromTempToFollowsRow struct {
	ID        int64
	AuthorID  int32
	SubjectID int32
}

func (q *Queries) InsertFromTempToFollows(ctx context.Context) ([]InsertFromTempToFollowsRow, error) {
	rows, err := q.db.Query(ctx, insertFromTempToFollows)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []InsertFromTempToFollowsRow
	for rows.Next() {
		var i InsertFromTempToFollowsRow
		if err := rows.Scan(&i.ID, &i.AuthorID, &i.SubjectID); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
