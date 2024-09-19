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
	Uri         string
	AuthorDid   string
	ReplyParent pgtype.Text
	ReplyRoot   pgtype.Text
	CreatedAt   pgtype.Timestamp
	Language    pgtype.Text
	Rank        pgtype.Float8
}

const bulkDeletePosts = `-- name: BulkDeletePosts :exec
DELETE
FROM posts
WHERE uri = ANY ($1::VARCHAR[])
`

func (q *Queries) BulkDeletePosts(ctx context.Context, dollar_1 []string) error {
	_, err := q.db.Exec(ctx, bulkDeletePosts, dollar_1)
	return err
}

const deleteOldPosts = `-- name: DeleteOldPosts :exec
DELETE
FROM posts
WHERE posts.created_at < current_timestamp - interval '10 days'
`

func (q *Queries) DeleteOldPosts(ctx context.Context) error {
	_, err := q.db.Exec(ctx, deleteOldPosts)
	return err
}

const deleteUserPosts = `-- name: DeleteUserPosts :exec
DELETE
FROM posts
WHERE author_did = $1
`

func (q *Queries) DeleteUserPosts(ctx context.Context, authorDid string) error {
	_, err := q.db.Exec(ctx, deleteUserPosts, authorDid)
	return err
}

const getLanguagePosts = `-- name: GetLanguagePosts :many
SELECT posts.uri, posts.author_did, posts.reply_parent, posts.reply_root, posts.indexed_at, posts.created_at, posts.language, posts.rank
FROM posts
WHERE language = $1
  AND reply_root IS NULL
  AND rank < $2
ORDER BY rank DESC
LIMIT $3
`

type GetLanguagePostsParams struct {
	Language pgtype.Text
	Rank     pgtype.Float8
	Limit    int32
}

func (q *Queries) GetLanguagePosts(ctx context.Context, arg GetLanguagePostsParams) ([]Post, error) {
	rows, err := q.db.Query(ctx, getLanguagePosts, arg.Language, arg.Rank, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Post
	for rows.Next() {
		var i Post
		if err := rows.Scan(
			&i.Uri,
			&i.AuthorDid,
			&i.ReplyParent,
			&i.ReplyRoot,
			&i.IndexedAt,
			&i.CreatedAt,
			&i.Language,
			&i.Rank,
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

const getLanguageTopPosts = `-- name: GetLanguageTopPosts :many
SELECT uri as uri,
       ''  as repost_uri,
       created_at,
       rank
FROM posts
         INNER JOIN users u ON posts.author_did = u.did
WHERE language = $1
  AND reply_root IS NULL
  AND u.followers_count > 300
  AND u.engagement_factor > 1
  AND rank < $2
ORDER BY rank DESC
LIMIT $3
`

type GetLanguageTopPostsParams struct {
	Language pgtype.Text
	Rank     pgtype.Float8
	Limit    int32
}

type GetLanguageTopPostsRow struct {
	Uri       string
	RepostUri string
	CreatedAt pgtype.Timestamp
	Rank      pgtype.Float8
}

func (q *Queries) GetLanguageTopPosts(ctx context.Context, arg GetLanguageTopPostsParams) ([]GetLanguageTopPostsRow, error) {
	rows, err := q.db.Query(ctx, getLanguageTopPosts, arg.Language, arg.Rank, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetLanguageTopPostsRow
	for rows.Next() {
		var i GetLanguageTopPostsRow
		if err := rows.Scan(
			&i.Uri,
			&i.RepostUri,
			&i.CreatedAt,
			&i.Rank,
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
