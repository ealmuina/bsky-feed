package models

import (
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"time"
)

type Post struct {
	Uri         string
	Reason      map[string]string
	Rank        float64
	CreatedAt   time.Time
	AuthorDid   string
	ReplyParent string
	ReplyRoot   string
	Language    string
	Text        string
	Embed       *appbsky.FeedPost_Embed
}
