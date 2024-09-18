package main

import (
	"bsky/cache"
	db "bsky/db/sqlc"
	"bsky/firehose"
	"bsky/server"
	"bsky/tasks"
	"bsky/utils"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"math"
	"net/url"
	"os"
)

func runBackgroundTasks(
	queries *db.Queries,
	usersCache *cache.UsersCache,
	timelinesCache *cache.TimelinesCache,
) {
	// DB cleanup
	go utils.Recoverer(math.MaxInt, 1, func() {
		tasks.CleanOldData(queries, timelinesCache)
	})

	// Firehose consumer
	go utils.Recoverer(math.MaxInt, 1, func() {
		subscription := firehose.NewSubscription(
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
		statisticsUpdater, err := tasks.NewStatisticsUpdater(queries, usersCache)
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

	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisOptions := redis.Options{
		Addr:     fmt.Sprintf("%s:%s", redisHost, redisPort),
		Password: "", // no password set
		DB:       0,  // use default DB
	}
	usersCache := cache.NewUsersCache(&redisOptions)
	timelinesCache := cache.NewTimelinesCache(&redisOptions)

	s := server.NewServer(queries, timelinesCache, usersCache)

	// Run background tasks
	runBackgroundTasks(queries, usersCache, timelinesCache)

	s.Run()
}
