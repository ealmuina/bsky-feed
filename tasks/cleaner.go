package tasks

import (
	"bsky/cache"
	db "bsky/db/sqlc"
	"bsky/server"
	"context"
	"time"
)

func cleanFeeds(timelinesCache *cache.TimelinesCache, feeds map[string]string) {
	for feedName := range feeds {
		timelinesCache.DeleteExpiredPosts(feedName, time.Now().Add(-7*24*time.Hour))
	}
}

func CleanOldData(queries *db.Queries, timelinesCache *cache.TimelinesCache) {
	ctx := context.Background()
	for {
		select {
		case <-time.After(24 * time.Hour):
			// Clean DB
			queries.DeleteOldPosts(ctx)
			queries.DeleteOldInteractions(ctx)

			// Clean caches
			cleanFeeds(timelinesCache, server.LanguageFeeds)
			cleanFeeds(timelinesCache, server.TopLanguageFeeds)
		}
	}
}
