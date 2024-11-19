package algorithms

import (
	"bsky/storage/cache"
	"bsky/storage/models"
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

func (a *KeywordsAlgorithm) AcceptsPost(post models.Post, _ cache.UserStatistics) (ok bool, reason map[string]string) {
	ok = post.Language == a.languageCode &&
		post.ReplyRoot == nil &&
		a.keywordsWeight(post.Text) > 1
	reason = nil
	return ok, reason
}
