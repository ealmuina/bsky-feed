package models

import (
	"gorm.io/gorm"
	"time"
)

type Post struct {
	gorm.Model

	Uri         string `gorm:"index"`
	Cid         string
	ReplyParent *string
	ReplyRoot   *string
	IndexedAt   time.Time
}
