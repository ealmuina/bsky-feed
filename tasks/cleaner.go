package tasks

import (
	"bsky/storage"
	"time"
)

func CleanOldData(storageManager *storage.Manager, persistentDb bool) {
	for {
		select {
		case <-time.After(1 * time.Hour):
			storageManager.CleanOldData(persistentDb)
		}
	}
}
