package algorithms

import (
	"bsky/cache"
	db "bsky/db/sqlc"
	"bsky/feeds"
	"context"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

const MinTopUserFollowers = 300
const MinTopUserEngagementFactor = 1.0

func GetLanguageAlgorithm(languageCode string) feeds.Algorithm {
	return func(
		params feeds.QueryParams,
		queries *db.Queries,
		ctx context.Context,
		cursorRank float64,
	) []feeds.Post {
		posts, err := queries.GetLanguagePosts(
			ctx,
			db.GetLanguagePostsParams{
				Language: pgtype.Text{String: languageCode, Valid: true},
				Rank:     pgtype.Float8{Float64: cursorRank, Valid: true},
				Limit:    int32(params.Limit),
			},
		)
		if err != nil {
			log.Errorf("error getting language posts: %v", err)
			return nil
		}
		result := make([]feeds.Post, len(posts))
		for i, post := range posts {
			result[i] = feeds.Post{
				Uri:  post.Uri,
				Rank: post.Rank.Float64,
			}
		}
		return result
	}
}

func GetLanguageAlgorithmAcceptance(languageCode string) feeds.AlgorithmAcceptance {
	return func(feed *feeds.Feed, post feeds.Post) (ok bool, reason map[string]string) {
		return post.Language == languageCode && post.ReplyRoot == "", nil
	}
}

func GetTopLanguageAlgorithm(languageCode string) feeds.Algorithm {
	return func(
		params feeds.QueryParams,
		queries *db.Queries,
		ctx context.Context,
		cursorRank float64,
	) []feeds.Post {
		posts, err := queries.GetLanguageTopPosts(
			ctx,
			db.GetLanguageTopPostsParams{
				Language: pgtype.Text{String: languageCode, Valid: true},
				Rank:     pgtype.Float8{Float64: cursorRank, Valid: true},
				Limit:    int32(params.Limit),
			},
		)
		if err != nil {
			log.Errorf("error getting top language posts: %v", err)
			return nil
		}

		result := make([]feeds.Post, len(posts))

		for i, post := range posts {
			var reason map[string]string = nil
			if post.RepostUri != "" {
				reason = map[string]string{
					"$type":  "app.bsky.feeds.defs#skeletonReasonRepost",
					"repost": post.RepostUri,
				}
			}
			result[i] = feeds.Post{
				Uri:    post.Uri,
				Rank:   post.Rank.Float64,
				Reason: reason,
			}
		}

		return result
	}
}

func GetTopLanguageAlgorithmAcceptance(languageCode string) feeds.AlgorithmAcceptance {
	return func(
		feed *feeds.Feed,
		post feeds.Post,
	) (ok bool, reason map[string]string) {
		// Bad language or reply. Skip directly
		if post.Language != languageCode || post.ReplyRoot != "" {
			return false, nil
		}

		// Get user stats
		// Check cache
		ok, cacheUser := feed.UsersCache.GetUser(post.AuthorDid)
		if !ok {
			// Not in cache. Get it from DB
			dbUser, err := feed.Queries.GetUser(context.Background(), post.AuthorDid)
			if err != nil {
				// Not in DB either
				return false, nil
			}
			if !dbUser.FollowersCount.Valid || !dbUser.EngagementFactor.Valid {
				// No statistics from user in DB
				return false, nil
			}
			// Fill cacheUser and store it in cache for future requests
			cacheUser = cache.User{
				Did:              dbUser.Did,
				FollowersCount:   int64(dbUser.FollowersCount.Int32),
				EngagementFactor: dbUser.EngagementFactor.Float64,
			}
			feed.UsersCache.AddUser(cacheUser)
		}

		// Apply filtering criteria
		ok = cacheUser.FollowersCount > MinTopUserFollowers && cacheUser.EngagementFactor > MinTopUserEngagementFactor
		reason = nil

		return
	}
}
