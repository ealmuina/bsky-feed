package algorithms

import (
	"bsky/storage/cache"
	"bsky/storage/models"
)

type LanguageAlgorithm struct {
	languageCode string
}

func (a *LanguageAlgorithm) AcceptsPost(post models.Post, _ cache.UserStatistics) (ok bool, reason map[string]string) {
	return post.Language == a.languageCode && post.ReplyRootId == 0, nil
}
