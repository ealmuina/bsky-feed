package algorithms

import (
	"bsky/storage/cache"
	"bsky/storage/db/models"
	"bsky/storage/utils"
	"github.com/scylladb/gocqlx/v3"
)

type LightLanguageAlgorithm struct {
	languageCode string
}

func (a *LightLanguageAlgorithm) AcceptsPost(
	postContent utils.PostContent,
	_ cache.UserStatistics,
) (ok bool, reason map[string]string) {
	ok = postContent.Post.Language == a.languageCode &&
		postContent.Post.ReplyRoot == "" &&
		postContent.Embed == nil
	reason = nil
	return
}

func (a *LightLanguageAlgorithm) GetPosts(_ *gocqlx.Session, _ float64, _ int64) []models.PostsStruct {
	// These timelines are stored in memory only
	return make([]models.PostsStruct, 0)
}
