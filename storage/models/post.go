package models

import (
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"time"
)

type Post struct {
	ID          int32
	UriKey      string
	Reason      map[string]string
	Rank        float64
	CreatedAt   time.Time
	AuthorId    int32
	ReplyParent []string
	ReplyRoot   []string
	Language    string
	Text        string
	Embed       *appbsky.FeedPost_Embed
}
