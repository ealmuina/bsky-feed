// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: users.sql

package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

const addUserFollowers = `-- name: AddUserFollowers :exec
UPDATE users
SET followers_count = followers_count + $2
WHERE id = $1
`

type AddUserFollowersParams struct {
	ID             int32
	FollowersCount pgtype.Int4
}

func (q *Queries) AddUserFollowers(ctx context.Context, arg AddUserFollowersParams) error {
	_, err := q.db.Exec(ctx, addUserFollowers, arg.ID, arg.FollowersCount)
	return err
}

const addUserFollows = `-- name: AddUserFollows :exec
UPDATE users
SET follows_count = follows_count + $2
WHERE id = $1
`

type AddUserFollowsParams struct {
	ID           int32
	FollowsCount pgtype.Int4
}

func (q *Queries) AddUserFollows(ctx context.Context, arg AddUserFollowsParams) error {
	_, err := q.db.Exec(ctx, addUserFollows, arg.ID, arg.FollowsCount)
	return err
}

const addUserPosts = `-- name: AddUserPosts :exec
UPDATE users
SET posts_count = users.posts_count + $2
WHERE id = $1
`

type AddUserPostsParams struct {
	ID         int32
	PostsCount pgtype.Int4
}

func (q *Queries) AddUserPosts(ctx context.Context, arg AddUserPostsParams) error {
	_, err := q.db.Exec(ctx, addUserPosts, arg.ID, arg.PostsCount)
	return err
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
WHERE id = $1
`

func (q *Queries) DeleteUser(ctx context.Context, id int32) error {
	_, err := q.db.Exec(ctx, deleteUser, id)
	return err
}

const getUser = `-- name: GetUser :one
SELECT id, did, handle, followers_count, follows_count, posts_count, last_update, refresh_frequency
FROM users
WHERE id = $1
LIMIT 1
`

func (q *Queries) GetUser(ctx context.Context, id int32) (User, error) {
	row := q.db.QueryRow(ctx, getUser, id)
	var i User
	err := row.Scan(
		&i.ID,
		&i.Did,
		&i.Handle,
		&i.FollowersCount,
		&i.FollowsCount,
		&i.PostsCount,
		&i.LastUpdate,
		&i.RefreshFrequency,
	)
	return i, err
}

const getUserDidsToRefreshStatistics = `-- name: GetUserDidsToRefreshStatistics :many
SELECT users.did
FROM users
WHERE last_update IS NULL
   OR last_update < current_timestamp - (refresh_frequency || ' days')::interval
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

const getUserId = `-- name: GetUserId :one
SELECT id
FROM users
WHERE did = $1
LIMIT 1
`

func (q *Queries) GetUserId(ctx context.Context, did string) (int32, error) {
	row := q.db.QueryRow(ctx, getUserId, did)
	var id int32
	err := row.Scan(&id)
	return id, err
}

const updateUser = `-- name: UpdateUser :exec
UPDATE users
SET handle            = $2,
    followers_count   = $3,
    follows_count     = $4,
    posts_count       = $5,
    last_update       = $6,
    refresh_frequency = greatest(1, 30 - (5 * log($3 + 1)))
WHERE did = $1
`

type UpdateUserParams struct {
	Did            string
	Handle         pgtype.Text
	FollowersCount pgtype.Int4
	FollowsCount   pgtype.Int4
	PostsCount     pgtype.Int4
	LastUpdate     pgtype.Timestamp
}

func (q *Queries) UpdateUser(ctx context.Context, arg UpdateUserParams) error {
	_, err := q.db.Exec(ctx, updateUser,
		arg.Did,
		arg.Handle,
		arg.FollowersCount,
		arg.FollowsCount,
		arg.PostsCount,
		arg.LastUpdate,
	)
	return err
}
