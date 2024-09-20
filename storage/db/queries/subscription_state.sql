-- name: GetSubscriptionState :one
SELECT *
FROM subscription_state
WHERE service = $1;

-- name: UpdateSubscriptionStateCursor :exec
INSERT INTO subscription_state (service, cursor)
VALUES ($1, $2)
ON CONFLICT (service)
    DO UPDATE SET cursor = $2;
