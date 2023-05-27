package main

import (
	"bsky/pkg/models"
	"bsky/pkg/subscription"
	"bsky/pkg/utils"
	"bsky/server"
	"math"
	"net/url"
)

func main() {
	db := models.Migrate(&models.MigrationConfig{
		DBPath: "test.db",
	})

	go utils.CleanOldData(
		db,
		[]interface{}{&models.Post{}, &models.Like{}},
	)

	go utils.Recoverer(math.MaxInt, 1, func() {
		firehoseSubscription := subscription.New(
			"test",
			db,
			url.URL{
				Scheme: "wss",
				Host:   "bsky.social",
				Path:   "/xrpc/com.atproto.sync.subscribeRepos",
			},
		)
		firehoseSubscription.Run()
	})

	s := server.Server{DB: db}
	s.Run()
}
