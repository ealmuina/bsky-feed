package models

import "time"

type InteractionType int16

const (
	Like   InteractionType = 0
	Repost InteractionType = 1
)

type Interaction struct {
	UriKey    string
	Kind      InteractionType
	AuthorId  int32
	PostId    int64
	CreatedAt time.Time
}
