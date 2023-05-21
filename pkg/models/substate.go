package models

import "gorm.io/gorm"

type SubState struct {
	gorm.Model

	Service string `gorm:"index"`
	Cursor  int64
}
