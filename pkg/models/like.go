package models

import (
	"gorm.io/gorm"
	"time"
)

type Like struct {
	gorm.Model

	Uri       string
	Cid       string
	ActorDid  string
	PostID    int
	Post      Post
	IndexedAt time.Time
}
