package algorithms

import (
	"bsky/storage/cache"
	"bsky/storage/db/models"
	"bsky/storage/db/queries"
	"bsky/storage/utils"
	"github.com/scylladb/gocqlx/v3"
	log "github.com/sirupsen/logrus"
)

type LanguageAlgorithm struct {
	languageCode string
}

func (a *LanguageAlgorithm) AcceptsPost(
	postContent utils.PostContent,
	_ cache.UserStatistics,
) (ok bool, reason map[string]string) {
	return postContent.Post.Language == a.languageCode && postContent.Post.ReplyRoot == "", nil
}

func (a *LanguageAlgorithm) GetPosts(session *gocqlx.Session, maxRank float64, limit int64) []models.PostsStruct {
	posts, err := queries.GetLanguagePosts(
		session, a.languageCode, maxRank, uint(limit),
	)
	if err != nil {
		log.Errorf("error getting language posts: %v", err)
		return nil
	}
	return posts
}
