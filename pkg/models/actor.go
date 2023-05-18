package models

import (
	"gorm.io/gorm"
)

type Actor struct {
	gorm.Model

	Did         string `gorm:"index"`
	Handle      string
	DisplayName *string
	Description *string
}
