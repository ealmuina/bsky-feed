package algorithms

import (
	"bsky/pkg/auth"
	"bsky/pkg/feed"
	"github.com/pemistahl/lingua-go"
	"gorm.io/gorm"
)

const PortugueseUri = "at://did:plc:qinqxdwwxgme6r4lgmkry5qu/app.bsky.feed.generator/portuguese"

func Portuguese(auth *auth.AuthConfig, db *gorm.DB, params feed.QueryParams) (string, []feed.SkeletonItem) {
	return getLanguageFeed(lingua.Portuguese)(auth, db, params)
}