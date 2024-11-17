package algorithms

import (
	"bsky/storage/cache"
	"bsky/storage/db/models"
	"bsky/storage/utils"
	"github.com/scylladb/gocqlx/v3"
)

type TopLanguageAlgorithm struct {
	languageCode  string
	minFollowers  int64
	minEngagement float64
}

func (a *TopLanguageAlgorithm) AcceptsPost(
	postContent utils.PostContent,
	authorStatistics cache.UserStatistics,
) (ok bool, reason map[string]string) {
	ok = postContent.Post.Language == a.languageCode &&
		postContent.Post.ReplyRoot == "" &&
		authorStatistics.FollowersCount > a.minFollowers &&
		authorStatistics.GetEngagementFactor() > a.minEngagement
	reason = nil
	return
}

func (a *TopLanguageAlgorithm) GetPosts(_ *gocqlx.Session, _ float64, _ int64) []models.PostsStruct {
	// These timelines are stored in memory only
	return make([]models.PostsStruct, 0)
}
