package algorithms

import (
	"bsky/storage/cache"
	"bsky/storage/models"
)

type Algorithm interface {
	AcceptsPost(post models.Post, authorStatistics cache.UserStatistics) (ok bool, reason map[string]string)
}

var ImplementedAlgorithms = map[string]Algorithm{
	"basque":     &LanguageAlgorithm{"eu"},
	"catalan":    &LanguageAlgorithm{"ca"},
	"galician":   &LanguageAlgorithm{"gl"},
	"portuguese": &LanguageAlgorithm{"pt"},
	"spanish":    &LanguageAlgorithm{"es"},
	"chinese":    &LanguageAlgorithm{"zh"},
	"top_spanish": &TopLanguageAlgorithm{
		languageCode:  "es",
		minFollowers:  1000,
		minEngagement: 1.0,
	},
	"top_portuguese": &TopLanguageAlgorithm{
		languageCode:  "pt",
		minFollowers:  1000,
		minEngagement: 1.0,
	},
	"light_spanish": &LightLanguageAlgorithm{"es"},
}
