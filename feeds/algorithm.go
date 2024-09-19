package feeds

import (
	db "bsky/db/sqlc"
	"context"
)

type Algorithm func(
	params QueryParams,
	queries *db.Queries,
	ctx context.Context,
	cursorRank float64,
) []Post

type AlgorithmAcceptance func(
	*Feed, Post,
) (ok bool, reason map[string]string)
