-- name: GetSubscriptionState :one
SELECT *
FROM subscription_state
WHERE service = $1;

-- name: UpdateSubscriptionStateCursor :exec
INSERT INTO subscription_state (service, cursor)
VALUES ($1, $2)
ON CONFLICT (service)
    DO UPDATE SET cursor = $2;

-- name: GetPdsSubscriptions :many
SELECT service
FROM subscription_state
WHERE service NOT IN ('firehose', 'backfill');