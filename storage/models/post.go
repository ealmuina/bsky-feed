package models

import (
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
}
