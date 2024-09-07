package main

import (
	db "bsky/db/sqlc"
	"bsky/firehose"
	"bsky/server"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/url"
	"os"
)

func main() {
	ctx := context.Background()
	connectionPool, err := pgxpool.New(
		ctx,
		fmt.Sprintf(
			"user=%s password=%s dbname=%s sslmode=disable host=%s port=%d",
			os.Getenv("DB_USERNAME"),
			os.Getenv("DB_PASSWORD"),
			"bsky_feeds",
			"localhost",
			5432,
		),
	)
	if err != nil {
		panic(err)
	}
	queries := db.New(connectionPool)
	s := server.New(queries)

	subscription := firehose.New(
		"bsky_feeds",
		queries,
		url.URL{
			Scheme: "wss",
			Host:   "bsky.network",
			Path:   "/xrpc/com.atproto.sync.subscribeRepos",
		},
	)
	go subscription.Run()

	//go utils.CleanOldData(
	//	db,
	//	[]interface{}{&models.Post{}, &models.Like{}},
	//)

	//go utils.Recoverer(math.MaxInt, 1, func() {
	//	firehoseSubscription := subscription.New(
	//		"test",
	//		db,
	//		url.URL{
	//			Scheme: "wss",
	//			Host:   "bsky.social",
	//			Path:   "/xrpc/com.atproto.sync.subscribeRepos",
	//		},
	//	)
	//	firehoseSubscription.Run()
	//})

	s.Run()
}
