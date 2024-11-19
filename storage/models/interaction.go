package models

import "time"

type InteractionType string

const (
	Like   InteractionType = "like"
	Repost InteractionType = "repost"
)

type Interaction struct {
	UriKey       string
	Kind         InteractionType
	AuthorID     int32
	PostUriKey   string
	PostAuthorId int32
	IndexedAt    time.Time
	CreatedAt    time.Time
}
