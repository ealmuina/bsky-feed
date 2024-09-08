package feed

import (
	"bsky/db/sqlc"
	"context"
)

type QueryParams struct {
	Limit  int
	Cursor *string
}

type Feed struct {
	queries   *db.Queries
	algorithm Algorithm
}

func New(queries *db.Queries, algorithm Algorithm) Feed {
	return Feed{
		queries:   queries,
		algorithm: algorithm,
	}
}

func (feed Feed) GetPosts(params QueryParams) Response {
	ctx := context.Background()
	return feed.algorithm(params, feed.queries, &ctx)
}

type Response struct {
	Cursor string `json:"cursor"`
	Posts  []Post `json:"feed"`
}

type Post struct {
	Uri string `json:"post"`
}

type Algorithm func(
	params QueryParams,
	queries *db.Queries,
	ctx *context.Context,
) Response
