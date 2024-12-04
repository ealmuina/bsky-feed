// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: subscription_state.sql

package db

import (
	"context"
)

const getPdsSubscriptions = `-- name: GetPdsSubscriptions :many
SELECT service
FROM subscription_state
WHERE service NOT IN ('firehose', 'backfill')
`

func (q *Queries) GetPdsSubscriptions(ctx context.Context) ([]string, error) {
	rows, err := q.db.Query(ctx, getPdsSubscriptions)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []string
	for rows.Next() {
		var service string
		if err := rows.Scan(&service); err != nil {
			return nil, err
		}
		items = append(items, service)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getSubscriptionState = `-- name: GetSubscriptionState :one
SELECT id, service, cursor
FROM subscription_state
WHERE service = $1
`

func (q *Queries) GetSubscriptionState(ctx context.Context, service string) (SubscriptionState, error) {
	row := q.db.QueryRow(ctx, getSubscriptionState, service)
	var i SubscriptionState
	err := row.Scan(&i.ID, &i.Service, &i.Cursor)
	return i, err
}

const updateSubscriptionStateCursor = `-- name: UpdateSubscriptionStateCursor :exec
INSERT INTO subscription_state (service, cursor)
VALUES ($1, $2)
ON CONFLICT (service)
    DO UPDATE SET cursor = $2
`

type UpdateSubscriptionStateCursorParams struct {
	Service string
	Cursor  string
}

func (q *Queries) UpdateSubscriptionStateCursor(ctx context.Context, arg UpdateSubscriptionStateCursorParams) error {
	_, err := q.db.Exec(ctx, updateSubscriptionStateCursor, arg.Service, arg.Cursor)
	return err
}
