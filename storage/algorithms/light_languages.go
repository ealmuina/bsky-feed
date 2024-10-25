package algorithms

import (
	"bsky/storage/cache"
	db "bsky/storage/db/sqlc"
	"bsky/storage/models"
)

type LightLanguageAlgorithm struct {
	languageCode string
}

func (a *LightLanguageAlgorithm) AcceptsPost(post models.Post, _ cache.UserStatistics) (ok bool, reason map[string]string) {
	ok = post.Language == a.languageCode &&
		post.ReplyRoot == "" &&
		post.Embed == nil
	reason = nil
	return
}

func (a *LightLanguageAlgorithm) GetPosts(_ *db.Queries, _ float64, _ int64) []models.Post {
	// These timelines are stored in memory only
	return make([]models.Post, 0)
}
