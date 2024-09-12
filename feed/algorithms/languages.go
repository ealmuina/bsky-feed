package algorithms

import (
	db "bsky/db/sqlc"
	"bsky/feed"
	"context"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
	"time"
)

func GetLanguageAlgorithm(languageCode string) feed.Algorithm {
	return func(
		params feed.QueryParams,
		queries *db.Queries,
		ctx *context.Context,
		createdAt time.Time,
		cid string,
	) []db.Post {
		posts, err := queries.GetLanguagePosts(
			*ctx,
			db.GetLanguagePostsParams{
				Language:  pgtype.Text{String: languageCode, Valid: true},
				CreatedAt: pgtype.Timestamp{Time: createdAt, Valid: true},
				Cid:       cid,
				Limit:     int32(params.Limit),
			},
		)
		if err != nil {
			log.Errorf("error getting language posts: %v", err)
			return nil
		}
		return posts
	}
}

func GetTopLanguageAlgorithm(languageCode string) feed.Algorithm {
	return func(
		params feed.QueryParams,
		queries *db.Queries,
		ctx *context.Context,
		createdAt time.Time,
		cid string,
	) []db.Post {
		posts, err := queries.GetLanguageTopPosts(
			*ctx,
			db.GetLanguageTopPostsParams{
				Language:  pgtype.Text{String: languageCode, Valid: true},
				CreatedAt: pgtype.Timestamp{Time: createdAt, Valid: true},
				Cid:       cid,
				Limit:     int32(params.Limit),
			},
		)
		if err != nil {
			log.Errorf("error getting top language posts: %v", err)
			return nil
		}
		return posts
	}
}
