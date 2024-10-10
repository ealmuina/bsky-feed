package algorithms

import (
	"bsky/storage/cache"
	db "bsky/storage/db/sqlc"
	"bsky/storage/models"
)

type Algorithm interface {
	AcceptsPost(post models.Post, authorStatistics cache.UserStatistics) (ok bool, reason map[string]string)

	GetPosts(queries *db.Queries, maxRank float64, limit int64) []models.Post
}

var ImplementedAlgorithms = map[string]Algorithm{
	"basque":     &LanguageAlgorithm{"eu"},
	"catalan":    &LanguageAlgorithm{"ca"},
	"galician":   &LanguageAlgorithm{"gl"},
	"portuguese": &LanguageAlgorithm{"pt"},
	"spanish":    &LanguageAlgorithm{"es"},
	"top_spanish": &TopLanguageAlgorithm{
		languageCode:  "es",
		minFollowers:  500,
		minEngagement: 1.0,
	},
	"top_portuguese": &TopLanguageAlgorithm{
		languageCode:  "pt",
		minFollowers:  10000,
		minEngagement: 1.0,
	},
	"us_election_es": &AiAlgorithm{
		languageCode: "es",
		prompt:       "Is this tweet related to the US 2024 election",
	},
}
