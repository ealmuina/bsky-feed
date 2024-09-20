package tasks

import (
	"bsky/storage"
	"time"
)

func CleanOldData(storageManager *storage.Manager) {
	for {
		select {
		case <-time.After(24 * time.Hour):
			storageManager.CleanOldData()
		}
	}
}
