package algorithms

import (
	"bsky/pkg/auth"
	"bsky/pkg/feed"
	"github.com/pemistahl/lingua-go"
	"gorm.io/gorm"
)

const SpanishUri = "at://did:plc:qinqxdwwxgme6r4lgmkry5qu/app.bsky.feed.generator/spanish"

func Spanish(auth *auth.AuthConfig, db *gorm.DB, params feed.QueryParams) (string, []feed.SkeletonItem) {
	return getLanguageFeed(lingua.Spanish)(auth, db, params)
}
