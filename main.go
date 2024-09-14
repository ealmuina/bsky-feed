package main

import (
	db "bsky/db/sqlc"
	"bsky/firehose"
	"bsky/server"
	"bsky/tasks"
	"bsky/utils"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
	"math"
	"net/url"
	"os"
)

func runBackgroundTasks(queries *db.Queries) {
	// DB cleanup
	go utils.Recoverer(math.MaxInt, 1, func() {
		utils.CleanOldData(queries)
	})

	// Firehose consumer
	go utils.Recoverer(math.MaxInt, 1, func() {
		subscription := firehose.New(
			"bsky_feeds",
			queries,
			url.URL{
				Scheme: "wss",
				Host:   "bsky.network",
				Path:   "/xrpc/com.atproto.sync.subscribeRepos",
			},
		)
		subscription.Run()
	})

	// Statistics updater
	go utils.Recoverer(math.MaxInt, 1, func() {
		statisticsUpdater, err := tasks.NewStatisticsUpdater(queries)
		if err != nil {
			panic(err)
		}
		statisticsUpdater.Run()
	})
}

func main() {
	log.SetLevel(log.WarnLevel)

	ctx := context.Background()
	connectionPool, err := pgxpool.New(
		ctx,
		fmt.Sprintf(
			"user=%s password=%s dbname=%s sslmode=disable host=%s port=%s",
			os.Getenv("DB_USERNAME"),
			os.Getenv("DB_PASSWORD"),
			"bsky_feeds",
			os.Getenv("DB_HOST"),
			os.Getenv("DB_PORT"),
		),
	)
	if err != nil {
		panic(err)
	}
	queries := db.New(connectionPool)
	s := server.New(queries)

	// Run background tasks
	runBackgroundTasks(queries)

	s.Run()
}
