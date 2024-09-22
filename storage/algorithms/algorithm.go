package algorithms

import (
	db "bsky/storage/db/sqlc"
	"bsky/storage/models"
)

type Algorithm interface {
	AcceptsPost(post models.Post, author models.User) (ok bool, reason map[string]string)

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
		minFollowers:  300,
		minEngagement: 1.0,
	},
}

var EngagementBasedAlgorithms = map[string]bool{
	"top_spanish": true,
}
