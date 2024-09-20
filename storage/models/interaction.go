package models

import "time"

type InteractionType string

const (
	Like   InteractionType = "like"
	Repost InteractionType = "repost"
)

type Interaction struct {
	Uri       string
	Kind      InteractionType
	AuthorDid string
	PostUri   string
	IndexedAt time.Time
	CreatedAt time.Time
}
