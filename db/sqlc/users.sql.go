// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: users.sql

package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

const calculateUserEngagement = `-- name: CalculateUserEngagement :one
SELECT (
           ((count(i.uri) / count(DISTINCT i.post_uri)::float) * 100 / u.followers_count) / (5 / log(u.followers_count))
           )::float
FROM interactions i
         INNER JOIN posts p ON i.post_uri = p.uri
         INNER JOIN users u ON u.did = p.author_did
WHERE p.author_did = $1
  AND p.reply_root IS NULL
  AND p.created_at < now() - interval '1 day'
  AND i.created_at > now() - interval '7 days'
GROUP BY u.followers_count
`

func (q *Queries) CalculateUserEngagement(ctx context.Context, authorDid string) (float64, error) {
	row := q.db.QueryRow(ctx, calculateUserEngagement, authorDid)
	var column_1 float64
	err := row.Scan(&column_1)
	return column_1, err
}

const createUser = `-- name: CreateUser :exec
INSERT INTO users (did, handle, followers_count, follows_count, posts_count, last_update)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT DO NOTHING
`

type CreateUserParams struct {
	Did            string
	Handle         pgtype.Text
	FollowersCount pgtype.Int4
	FollowsCount   pgtype.Int4
	PostsCount     pgtype.Int4
	LastUpdate     pgtype.Timestamp
}

func (q *Queries) CreateUser(ctx context.Context, arg CreateUserParams) error {
	_, err := q.db.Exec(ctx, createUser,
		arg.Did,
		arg.Handle,
		arg.FollowersCount,
		arg.FollowsCount,
		arg.PostsCount,
		arg.LastUpdate,
	)
	return err
}

const deleteUser = `-- name: DeleteUser :exec
DELETE
FROM users
WHERE did = $1
`

func (q *Queries) DeleteUser(ctx context.Context, did string) error {
	_, err := q.db.Exec(ctx, deleteUser, did)
	return err
}

const getUser = `-- name: GetUser :one
SELECT did, handle, followers_count, follows_count, posts_count, indexed_at, last_update, engagement_factor
FROM users
WHERE did = $1
LIMIT 1
`

func (q *Queries) GetUser(ctx context.Context, did string) (User, error) {
	row := q.db.QueryRow(ctx, getUser, did)
	var i User
	err := row.Scan(
		&i.Did,
		&i.Handle,
		&i.FollowersCount,
		&i.FollowsCount,
		&i.PostsCount,
		&i.IndexedAt,
		&i.LastUpdate,
		&i.EngagementFactor,
	)
	return i, err
}

const getUserDids = `-- name: GetUserDids :many
SELECT users.did
FROM users
`

func (q *Queries) GetUserDids(ctx context.Context) ([]string, error) {
	rows, err := q.db.Query(ctx, getUserDids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []string
	for rows.Next() {
		var did string
		if err := rows.Scan(&did); err != nil {
			return nil, err
		}
		items = append(items, did)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getUserDidsToRefreshStatistics = `-- name: GetUserDidsToRefreshStatistics :many
SELECT users.did
FROM users
WHERE last_update IS NULL
   OR last_update < current_timestamp - interval '1 day'
`

func (q *Queries) GetUserDidsToRefreshStatistics(ctx context.Context) ([]string, error) {
	rows, err := q.db.Query(ctx, getUserDidsToRefreshStatistics)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []string
	for rows.Next() {
		var did string
		if err := rows.Scan(&did); err != nil {
			return nil, err
		}
		items = append(items, did)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getUsersStatistics = `-- name: GetUsersStatistics :many
SELECT did, followers_count, engagement_factor
FROM users
WHERE followers_count IS NOT NULL
  AND engagement_factor IS NOT NULL
`

type GetUsersStatisticsRow struct {
	Did              string
	FollowersCount   pgtype.Int4
	EngagementFactor pgtype.Float8
}

func (q *Queries) GetUsersStatistics(ctx context.Context) ([]GetUsersStatisticsRow, error) {
	rows, err := q.db.Query(ctx, getUsersStatistics)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetUsersStatisticsRow
	for rows.Next() {
		var i GetUsersStatisticsRow
		if err := rows.Scan(&i.Did, &i.FollowersCount, &i.EngagementFactor); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const updateUser = `-- name: UpdateUser :exec
UPDATE users
SET handle            = $2,
    followers_count   = $3,
    follows_count     = $4,
    posts_count       = $5,
    engagement_factor = $6,
    last_update       = $7
WHERE did = $1
`

type UpdateUserParams struct {
	Did              string
	Handle           pgtype.Text
	FollowersCount   pgtype.Int4
	FollowsCount     pgtype.Int4
	PostsCount       pgtype.Int4
	EngagementFactor pgtype.Float8
	LastUpdate       pgtype.Timestamp
}

func (q *Queries) UpdateUser(ctx context.Context, arg UpdateUserParams) error {
	_, err := q.db.Exec(ctx, updateUser,
		arg.Did,
		arg.Handle,
		arg.FollowersCount,
		arg.FollowsCount,
		arg.PostsCount,
		arg.EngagementFactor,
		arg.LastUpdate,
	)
	return err
}
