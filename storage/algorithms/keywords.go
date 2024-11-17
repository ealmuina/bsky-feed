package algorithms

import (
	"bsky/storage/cache"
	"bsky/storage/db/models"
	"bsky/storage/utils"
	"github.com/scylladb/gocqlx/v3"
	"regexp"
	"strings"
)

type KeywordsAlgorithm struct {
	languageCode string
	keywords     map[string]float64
}

func (a *KeywordsAlgorithm) keywordsWeight(text string) float64 {
	text = strings.ToLower(text)
	total := 0.0

	for keyword, weight := range a.keywords {
		if regexp.MustCompile(keyword).MatchString(text) {
			total += weight
		}
	}

	return total
}

func (a *KeywordsAlgorithm) AcceptsPost(
	postContent utils.PostContent,
	_ cache.UserStatistics,
) (ok bool, reason map[string]string) {
	ok = postContent.Post.Language == a.languageCode &&
		postContent.Post.ReplyRoot == "" &&
		a.keywordsWeight(postContent.Text) > 1
	reason = nil
	return ok, reason
}

func (a *KeywordsAlgorithm) GetPosts(_ *gocqlx.Session, _ float64, _ int64) []models.PostsStruct {
	// These timelines are stored in memory only
	return make([]models.PostsStruct, 0)
}
