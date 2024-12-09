package models

import (
	"fmt"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type Post struct {
	Id            primitive.ObjectID      `bson:"_id"`
	UriKey        string                  `bson:"uri_key"`
	Rank          float64                 `bson:"-"`
	CreatedAt     time.Time               `bson:"created_at"`
	AuthorId      string                  `bson:"author_id"`
	AuthorDid     string                  `bson:"-"`
	ReplyParentId string                  `bson:"reply_parent_id"`
	ReplyRootId   string                  `bson:"reply_root_id"`
	Language      string                  `bson:"language"`
	Text          string                  `bson:"-"`
	Embed         *appbsky.FeedPost_Embed `bson:"-"`
}

func (p *Post) Uri() string {
	return fmt.Sprintf("%s/app.bsky.feed.post/%s", p.AuthorDid, p.UriKey)
}
