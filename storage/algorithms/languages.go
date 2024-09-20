package algorithms

import (
	db "bsky/storage/db/sqlc"
	"bsky/storage/models"
	"context"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type LanguageAlgorithm struct {
	languageCode string
}

func (a *LanguageAlgorithm) AcceptsPost(post models.Post, _ models.User) (ok bool, reason map[string]string) {
	return post.Language == a.languageCode && post.ReplyRoot == "", nil
}

func (a *LanguageAlgorithm) GetPosts(queries *db.Queries, maxRank float64, limit int64) []models.Post {
	posts, err := queries.GetLanguagePosts(
		context.Background(),
		db.GetLanguagePostsParams{
			Language: pgtype.Text{String: a.languageCode, Valid: true},
			Rank:     pgtype.Float8{Float64: maxRank, Valid: true},
			Limit:    int32(limit),
		},
	)
	if err != nil {
		log.Errorf("error getting language posts: %v", err)
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
