// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: follows.sql

package db

import (
	"context"
)

type BulkCreateFollowsParams struct {
	UriKey    string
	AuthorID  int32
	SubjectID int32
}

const bulkDeleteFollows = `-- name: BulkDeleteFollows :many
DELETE
FROM follows
WHERE uri_key = ANY ($1::VARCHAR[])
  AND author_id = ANY ($2::INT[])
RETURNING id, author_id, subject_id
`

type BulkDeleteFollowsParams struct {
	UriKeys   []string
	AuthorIds []int32
}

type BulkDeleteFollowsRow struct {
	ID        int32
	AuthorID  int32
	SubjectID int32
}

func (q *Queries) BulkDeleteFollows(ctx context.Context, arg BulkDeleteFollowsParams) ([]BulkDeleteFollowsRow, error) {
	rows, err := q.db.Query(ctx, bulkDeleteFollows, arg.UriKeys, arg.AuthorIds)
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
SELECT id, uri_key, author_id, subject_id
FROM follows
    WITH NO DATA
`

func (q *Queries) CreateTempFollowsTable(ctx context.Context) error {
	_, err := q.db.Exec(ctx, createTempFollowsTable)
	return err
}

const insertFromTempToFollows = `-- name: InsertFromTempToFollows :many
INSERT INTO follows (uri_key, author_id, subject_id)
SELECT uri_key, author_id, subject_id
FROM tmp_follows
ON CONFLICT DO NOTHING
RETURNING uri_key, author_id, subject_id
`

type InsertFromTempToFollowsRow struct {
	UriKey    string
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
		if err := rows.Scan(&i.UriKey, &i.AuthorID, &i.SubjectID); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
