package algorithms

import (
	"bsky/pkg/auth"
	"bsky/pkg/feed"
	"github.com/pemistahl/lingua-go"
	"gorm.io/gorm"
)

const CatalanUri = "at://did:plc:qinqxdwwxgme6r4lgmkry5qu/app.bsky.feed.generator/catalan"

func Catalan(auth *auth.AuthConfig, db *gorm.DB, params feed.QueryParams) (string, []feed.SkeletonItem) {
	return getLanguageFeed(lingua.Catalan)(auth, db, params)
}
