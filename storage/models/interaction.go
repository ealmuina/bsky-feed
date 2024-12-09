package models

import "time"

type InteractionType int16

const (
	Like   InteractionType = 0
	Repost InteractionType = 1
)

type Interaction struct {
	UriKey    string          `bson:"uri_key"`
	Kind      InteractionType `bson:"kind"`
	AuthorId  string          `bson:"author_id"`
	PostId    string          `bson:"post_id"`
	CreatedAt time.Time       `bson:"created_at"`
}
