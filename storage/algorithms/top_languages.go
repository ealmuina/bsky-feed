package algorithms

import (
	"bsky/storage/cache"
	"bsky/storage/models"
)

type TopLanguageAlgorithm struct {
	languageCode  string
	minFollowers  int32
	minEngagement float64
}

func (a *TopLanguageAlgorithm) AcceptsPost(
	post models.Post,
	authorStatistics cache.UserStatistics,
) (ok bool, reason map[string]string) {
	ok = post.Language == a.languageCode &&
		post.ReplyRoot == nil &&
		authorStatistics.FollowersCount > a.minFollowers &&
		authorStatistics.GetEngagementFactor() > a.minEngagement
	reason = nil
	return
}
