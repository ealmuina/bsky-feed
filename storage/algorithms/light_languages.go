package algorithms

import (
	"bsky/storage/cache"
	"bsky/storage/models"
)

type LightLanguageAlgorithm struct {
	languageCode string
}

func (a *LightLanguageAlgorithm) AcceptsPost(post models.Post, _ cache.UserStatistics) (ok bool, reason map[string]string) {
	ok = post.Language == a.languageCode &&
		post.ReplyRootId == "" &&
		post.Embed == nil
	reason = nil
	return
}
