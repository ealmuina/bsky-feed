package models

import "time"

type InteractionType int16

const (
	Like   InteractionType = 0
	Repost InteractionType = 1
)

type Interaction struct {
	UriKey       string
	Kind         InteractionType
	AuthorId     int32
	PostUriKey   string
	PostAuthorId int32
	CreatedAt    time.Time
}
