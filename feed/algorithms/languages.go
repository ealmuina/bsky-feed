package algorithms

import (
	db "bsky/db/sqlc"
	"bsky/feed"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
)

const CursorEOF = "eof"

func GetLanguageAlgorithm(languageCode string) feed.Algorithm {
	return func(params feed.QueryParams, queries *db.Queries, ctx *context.Context) feed.Response {
		if params.Cursor == nil {
			*params.Cursor = fmt.Sprintf("%d::0", time.Now().Unix())
		} else if *params.Cursor == CursorEOF {
			return feed.Response{
				Cursor: CursorEOF,
				Posts:  make([]feed.Post, 0),
			}
		}

		cursorParts := strings.Split(*params.Cursor, "::")
		if len(cursorParts) != 2 {
			log.Errorf("Malformed cursor in %+v", params)
			return feed.Response{}
		}

		createdAtInt, _ := strconv.ParseInt(cursorParts[0], 10, 64)
		createdAt := time.Unix(createdAtInt, 0)
		cid := cursorParts[1]

		posts, err := queries.GetLanguagePosts(
			*ctx,
			db.GetLanguagePostsParams{
				Code:      languageCode,
				CreatedAt: pgtype.Timestamp{Time: createdAt, Valid: true},
				Cid:       cid,
				Limit:     int32(params.Limit),
			},
		)
		if err != nil {
			panic(err)
		}

		result := make([]feed.Post, len(posts))
		for i, post := range posts {
			result[i] = feed.Post{
				Uri: post.Uri,
			}
		}

		cursor := CursorEOF
		if len(result) > 0 {
			lastPost := posts[len(posts)-1]
			cursor = fmt.Sprintf(
				"%d::%s",
				lastPost.CreatedAt.Time.Unix(),
				lastPost.Cid,
			)
		}

		return feed.Response{
			Cursor: cursor,
			Posts:  result,
		}
	}
}
