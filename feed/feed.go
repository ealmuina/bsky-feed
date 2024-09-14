package feed

import (
	"bsky/db/sqlc"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
)

const CursorEOF = "eof"

type QueryParams struct {
	Limit  int
	Cursor string
}

type Feed struct {
	queries   *db.Queries
	algorithm Algorithm
}

type Response struct {
	Cursor string `json:"cursor"`
	Posts  []Post `json:"feed"`
}

type Post struct {
	Uri       string            `json:"post"`
	Reason    map[string]string `json:"reason"`
	CreatedAt time.Time         `json:"-"`
	Cid       string            `json:"-"`
}

type Algorithm func(
	params QueryParams,
	queries *db.Queries,
	ctx *context.Context,
	createdAt time.Time,
	cid string,
) []Post

func New(queries *db.Queries, algorithm Algorithm) *Feed {
	return &Feed{
		queries:   queries,
		algorithm: algorithm,
	}
}

func (feed *Feed) GetPosts(params QueryParams) Response {
	ctx := context.Background()

	if params.Cursor == "" {
		params.Cursor = fmt.Sprintf("%d::0", time.Now().Unix())
	} else if params.Cursor == CursorEOF {
		return Response{
			Cursor: CursorEOF,
			Posts:  make([]Post, 0),
		}
	}

	cursorParts := strings.Split(params.Cursor, "::")
	if len(cursorParts) != 2 {
		log.Errorf("Malformed cursor in %+v", params)
		return Response{}
	}

	createdAtInt, _ := strconv.ParseInt(cursorParts[0], 10, 64)
	createdAt := time.Unix(createdAtInt, 0)
	cid := cursorParts[1]

	posts := feed.algorithm(params, feed.queries, &ctx, createdAt, cid)

	cursor := CursorEOF
	if len(posts) > 0 {
		lastPost := posts[len(posts)-1]
		cursor = fmt.Sprintf(
			"%d::%s",
			lastPost.CreatedAt.Unix(),
			lastPost.Cid,
		)
	}

	return Response{
		Cursor: cursor,
		Posts:  posts,
	}
}
