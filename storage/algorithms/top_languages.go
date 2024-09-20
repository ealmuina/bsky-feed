package algorithms

import (
	db "bsky/storage/db/sqlc"
	"bsky/storage/models"
	"context"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type TopLanguageAlgorithm struct {
	languageCode  string
	minFollowers  int64
	minEngagement float64
}

func (a *TopLanguageAlgorithm) AcceptsPost(post models.Post, author models.User) (ok bool, reason map[string]string) {
	ok = post.Language == a.languageCode &&
		post.ReplyRoot == "" &&
		author.FollowersCount > a.minFollowers &&
		author.EngagementFactor > a.minEngagement
	reason = nil
	return
}

func (a *TopLanguageAlgorithm) GetPosts(queries *db.Queries, maxRank float64, limit int64) []models.Post {
	posts, err := queries.GetLanguageTopPosts(
		context.Background(),
		db.GetLanguageTopPostsParams{
			Language: pgtype.Text{String: a.languageCode, Valid: true},
			Rank:     pgtype.Float8{Float64: maxRank, Valid: true},
			Limit:    int32(limit),
		},
	)
	if err != nil {
		log.Errorf("error getting top language posts: %v", err)
		return nil
	}

	result := make([]models.Post, len(posts))
	for i, post := range posts {
		result[i] = models.Post{
			Uri:  post.Uri,
			Rank: post.Rank.Float64,
		}
	}
	return result
}
