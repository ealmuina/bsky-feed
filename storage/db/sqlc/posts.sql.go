// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: posts.sql

package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

type BulkCreatePostsParams struct {
	UriKey      string
	AuthorID    int32
	ReplyParent []string
	ReplyRoot   []string
	CreatedAt   pgtype.Timestamp
	Language    pgtype.Text
}

const bulkDeletePosts = `-- name: BulkDeletePosts :many
DELETE
FROM posts
WHERE uri_key = ANY ($1::VARCHAR[])
  AND author_id = ANY ($2::INT[])
RETURNING id, author_id
`

type BulkDeletePostsParams struct {
	UriKeys   []string
	AuthorIds []int32
}

type BulkDeletePostsRow struct {
	ID       int32
	AuthorID int32
}

func (q *Queries) BulkDeletePosts(ctx context.Context, arg BulkDeletePostsParams) ([]BulkDeletePostsRow, error) {
	rows, err := q.db.Query(ctx, bulkDeletePosts, arg.UriKeys, arg.AuthorIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []BulkDeletePostsRow
	for rows.Next() {
		var i BulkDeletePostsRow
		if err := rows.Scan(&i.ID, &i.AuthorID); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const createTempPostsTable = `-- name: CreateTempPostsTable :exec
CREATE TEMPORARY TABLE tmp_posts
    ON COMMIT DROP
AS
SELECT id, uri_key, author_id, reply_parent, reply_root, language, created_at
FROM posts
    WITH NO DATA
`

func (q *Queries) CreateTempPostsTable(ctx context.Context) error {
	_, err := q.db.Exec(ctx, createTempPostsTable)
	return err
}

const deleteUserPosts = `-- name: DeleteUserPosts :exec
DELETE
FROM posts
WHERE author_id = $1
`

func (q *Queries) DeleteUserPosts(ctx context.Context, authorID int32) error {
	_, err := q.db.Exec(ctx, deleteUserPosts, authorID)
	return err
}

const getOldPosts = `-- name: GetOldPosts :many
SELECT id, author_id
FROM posts
WHERE posts.created_at < current_timestamp - interval '7 days'
`

type GetOldPostsRow struct {
	ID       int32
	AuthorID int32
}

func (q *Queries) GetOldPosts(ctx context.Context) ([]GetOldPostsRow, error) {
	rows, err := q.db.Query(ctx, getOldPosts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetOldPostsRow
	for rows.Next() {
		var i GetOldPostsRow
		if err := rows.Scan(&i.ID, &i.AuthorID); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const insertFromTempToPosts = `-- name: InsertFromTempToPosts :many
INSERT INTO posts (uri_key, author_id, reply_parent, reply_root, created_at, language)
SELECT uri_key, author_id, reply_parent, reply_root, created_at, language
FROM tmp_posts
ON CONFLICT DO NOTHING
RETURNING id, author_id, reply_root
`

type InsertFromTempToPostsRow struct {
	ID        int32
	AuthorID  int32
	ReplyRoot []string
}

func (q *Queries) InsertFromTempToPosts(ctx context.Context) ([]InsertFromTempToPostsRow, error) {
	rows, err := q.db.Query(ctx, insertFromTempToPosts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []InsertFromTempToPostsRow
	for rows.Next() {
		var i InsertFromTempToPostsRow
		if err := rows.Scan(&i.ID, &i.AuthorID, &i.ReplyRoot); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
