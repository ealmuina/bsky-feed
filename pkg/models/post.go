package models

import (
	"gorm.io/gorm"
	"time"
)

type Post struct {
	gorm.Model

	Uri         string `gorm:"uniqueIndex"`
	Cid         string
	ReplyParent *string
	ReplyRoot   *string
	IndexedAt   time.Time

	Language string `gorm:"index"`
}
