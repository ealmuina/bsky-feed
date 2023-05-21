package utils

import (
	"gorm.io/gorm"
	"time"
)

func CleanOldData(db *gorm.DB, tables []any) {
	for {
		select {
		case <-time.After(1 * time.Hour):
			now := time.Now()
			for _, table := range tables {
				db.Unscoped().Delete(
					table,
					"created_at < ?",
					now.Add(-48*time.Hour),
				)
			}
		}
	}
}
