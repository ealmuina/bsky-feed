package main

import (
	"bsky/pkg/models"
	"bsky/pkg/subscription"
	"net/url"
)

func main() {
	db := models.Migrate(&models.MigrationConfig{DBPath: "test.db"})

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
}
