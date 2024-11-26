package models

import (
	"fmt"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"time"
)

type Post struct {
	ID            int64
	UriKey        string
	Rank          float64
	CreatedAt     time.Time
	AuthorId      int32
	AuthorDid     string
	ReplyParentId int64
	ReplyRootId   int64
	Language      string
	Text          string
	Embed         *appbsky.FeedPost_Embed
}

func (p *Post) Uri() string {
	return fmt.Sprintf("%s/app.bsky.feed.post/%s", p.AuthorDid, p.UriKey)
}
