package feeds

import (
	db "bsky/db/sqlc"
	"context"
	"time"
)

type Algorithm func(
	params QueryParams,
	queries *db.Queries,
	ctx context.Context,
	createdAt time.Time,
	cid string,
) []Post

type AlgorithmAcceptance func(
	*Feed, Post,
) (ok bool, reason map[string]string)
