package algorithms

import (
	"bsky/pkg/auth"
	"bsky/pkg/feed"
	"github.com/pemistahl/lingua-go"
	"gorm.io/gorm"
)

func getLanguageFeed(language lingua.Language) feed.Algorithm {
	return func(auth *auth.AuthConfig, db *gorm.DB, params feed.QueryParams) (string, []feed.SkeletonItem) {
		strLanguage := language.String()
		params.Language = &strLanguage

		cursor, posts := feed.GetFeed(db, params, func(query *gorm.DB) *gorm.DB {
			// Exclude replies
			return query.Where("reply_root IS NULL")
		})

		var result = make([]feed.SkeletonItem, 0)
		for _, post := range posts {
			result = append(result, feed.SkeletonItem{Post: post})
		}

		return cursor, result
	}
}
