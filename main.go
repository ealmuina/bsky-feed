package main

import (
	"bsky/firehose"
	"bsky/monitoring"
	"bsky/server"
	"bsky/storage"
	"bsky/tasks"
	"bsky/utils"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"math"
	"net/url"
	"os"
)

func init() {
	// Register Prometheus metrics
	prometheus.MustRegister(
		monitoring.HttpRequestsTotal,
		monitoring.HttpRequestDuration,
		monitoring.ActiveConnections,
		monitoring.FirehoseEvents,
		monitoring.FirehoseEventProcessingDuration,
	)
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

	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisClient := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", redisHost, redisPort),
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	storageManager := storage.NewManager(connectionPool, redisClient)

	// Run background tasks
	runBackgroundTasks(storageManager)

	// Run server
	s := server.NewServer(storageManager)
	s.Run()
}

func runBackgroundTasks(storageManager *storage.Manager) {
	// DB cleanup
	go utils.Recoverer(math.MaxInt, 1, func() {
		tasks.CleanOldData(storageManager)
	})

	// Firehose consumer
	go utils.Recoverer(math.MaxInt, 1, func() {
		subscription := firehose.NewSubscription(
			"bsky_feeds",
			url.URL{
				Scheme: "wss",
				Host:   "jetstream2.us-east.bsky.network",
				Path:   "/subscribe",
			},
			storageManager,
		)
		subscription.Run()
	})

	// Statistics updater
	go utils.Recoverer(math.MaxInt, 1, func() {
		statisticsUpdater, err := tasks.NewStatisticsUpdater(storageManager)
		if err != nil {
			panic(err)
		}
		statisticsUpdater.Run()
	})
}
