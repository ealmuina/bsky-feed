package tasks

import (
	db "bsky/db/sqlc"
	"context"
	"time"
)

func CleanOldData(queries *db.Queries) {
	ctx := context.Background()
	for {
		select {
		case <-time.After(24 * time.Hour):
			queries.DeleteOldPosts(ctx)
			queries.DeleteOldInteractions(ctx)
		}
	}
}
