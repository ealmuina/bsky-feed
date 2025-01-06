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
	UriKey        string
	AuthorID      int32
	ReplyParentID pgtype.Int8
	ReplyRootID   pgtype.Int8
	CreatedAt     pgtype.Timestamp
	Language      pgtype.Text
}

const bulkDeletePosts = `-- name: BulkDeletePosts :many
DELETE
FROM posts
WHERE author_id = ANY ($1::INT[])
  AND uri_key = ANY ($2::VARCHAR[])
RETURNING id, author_id, uri_key
`

type BulkDeletePostsParams struct {
	AuthorIds []int32
	UriKeys   []string
}

type BulkDeletePostsRow struct {
	ID       int64
	AuthorID int32
	UriKey   string
}

func (q *Queries) BulkDeletePosts(ctx context.Context, arg BulkDeletePostsParams) ([]BulkDeletePostsRow, error) {
	rows, err := q.db.Query(ctx, bulkDeletePosts, arg.AuthorIds, arg.UriKeys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []BulkDeletePostsRow
	for rows.Next() {
		var i BulkDeletePostsRow
		if err := rows.Scan(&i.ID, &i.AuthorID, &i.UriKey); err != nil {
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
SELECT id, uri_key, author_id, reply_parent_id, reply_root_id, language, created_at
FROM posts
    WITH NO DATA
`

func (q *Queries) CreateTempPostsTable(ctx context.Context) error {
	_, err := q.db.Exec(ctx, createTempPostsTable)
	return err
}

const deletePost = `-- name: DeletePost :one
DELETE
FROM posts
WHERE author_id = $1
  AND uri_key = $2
RETURNING id, author_id, uri_key, reply_root_id
`

type DeletePostParams struct {
	AuthorID int32
	UriKey   string
}

type DeletePostRow struct {
	ID          int64
	AuthorID    int32
	UriKey      string
	ReplyRootID pgtype.Int8
}

func (q *Queries) DeletePost(ctx context.Context, arg DeletePostParams) (DeletePostRow, error) {
	row := q.db.QueryRow(ctx, deletePost, arg.AuthorID, arg.UriKey)
	var i DeletePostRow
	err := row.Scan(
		&i.ID,
		&i.AuthorID,
		&i.UriKey,
		&i.ReplyRootID,
	)
	return i, err
}

const getOldPosts = `-- name: GetOldPosts :many
SELECT id, author_id, reply_root_id
FROM posts
WHERE posts.created_at < current_timestamp - interval '7 days'
`

type GetOldPostsRow struct {
	ID          int64
	AuthorID    int32
	ReplyRootID pgtype.Int8
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
		if err := rows.Scan(&i.ID, &i.AuthorID, &i.ReplyRootID); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getPostAuthorId = `-- name: GetPostAuthorId :one
SELECT author_id
FROM posts
WHERE id = $1
`

func (q *Queries) GetPostAuthorId(ctx context.Context, id int64) (int32, error) {
	row := q.db.QueryRow(ctx, getPostAuthorId, id)
	var author_id int32
	err := row.Scan(&author_id)
	return author_id, err
}

const getPostId = `-- name: GetPostId :one
SELECT id
FROM posts
WHERE author_id = $1
  AND uri_key = $2
`

type GetPostIdParams struct {
	AuthorID int32
	UriKey   string
}

func (q *Queries) GetPostId(ctx context.Context, arg GetPostIdParams) (int64, error) {
	row := q.db.QueryRow(ctx, getPostId, arg.AuthorID, arg.UriKey)
	var id int64
	err := row.Scan(&id)
	return id, err
}

const getUserPosts = `-- name: GetUserPosts :many
SELECT id, uri_key, author_id
FROM posts
WHERE author_id = $1
`

type GetUserPostsRow struct {
	ID       int64
	UriKey   string
	AuthorID int32
}

func (q *Queries) GetUserPosts(ctx context.Context, authorID int32) ([]GetUserPostsRow, error) {
	rows, err := q.db.Query(ctx, getUserPosts, authorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetUserPostsRow
	for rows.Next() {
		var i GetUserPostsRow
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

const insertFromTempToPosts = `-- name: InsertFromTempToPosts :many
INSERT INTO posts (uri_key, author_id, reply_parent_id, reply_root_id, created_at, language)
SELECT uri_key, author_id, reply_parent_id, reply_root_id, created_at, language
FROM tmp_posts
ON CONFLICT DO NOTHING
RETURNING id, author_id, reply_root_id
`

type InsertFromTempToPostsRow struct {
	ID          int64
	AuthorID    int32
	ReplyRootID pgtype.Int8
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
		if err := rows.Scan(&i.ID, &i.AuthorID, &i.ReplyRootID); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const upsertPost = `-- name: UpsertPost :one
INSERT INTO posts (uri_key, author_id, reply_parent_id, reply_root_id, created_at, language)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (author_id, uri_key) DO UPDATE
    SET reply_parent_id = COALESCE(posts.reply_parent_id, excluded.reply_parent_id),
        reply_root_id   = COALESCE(posts.reply_root_id, excluded.reply_root_id),
        created_at      = COALESCE(posts.created_at, excluded.created_at),
        language        = COALESCE(posts.language, excluded.language)
RETURNING id, XMAX = 0 AS is_created
`

type UpsertPostParams struct {
	UriKey        string
	AuthorID      int32
	ReplyParentID pgtype.Int8
	ReplyRootID   pgtype.Int8
	CreatedAt     pgtype.Timestamp
	Language      pgtype.Text
}

type UpsertPostRow struct {
	ID        int64
	IsCreated bool
}

func (q *Queries) UpsertPost(ctx context.Context, arg UpsertPostParams) (UpsertPostRow, error) {
	row := q.db.QueryRow(ctx, upsertPost,
		arg.UriKey,
		arg.AuthorID,
		arg.ReplyParentID,
		arg.ReplyRootID,
		arg.CreatedAt,
		arg.Language,
	)
	var i UpsertPostRow
	err := row.Scan(&i.ID, &i.IsCreated)
	return i, err
}
