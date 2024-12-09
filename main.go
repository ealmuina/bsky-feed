package main

import (
	"bsky/backfill"
	"bsky/firehose"
	"bsky/monitoring"
	"bsky/server"
	"bsky/storage"
	"bsky/tasks"
	"bsky/utils"
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"math"
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
	mongoUri := os.Getenv("MONGODB_URI")
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoUri))
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

	storageManager := storage.NewManager(
		client.Database("bsky_feeds"),
		redisClient,
		os.Getenv("PERSIST_FOLLOWS") == "true",
	)

	// Run background tasks
	runBackgroundTasks(storageManager)

	// Run server
	s := server.NewServer(storageManager)
	s.Run()
}

func runBackgroundTasks(storageManager *storage.Manager) {
	runBackfill := os.Getenv("RUN_BACKFILL") == "true"

	// DB cleanup
	go utils.Recoverer(math.MaxInt, 1, func() {
		tasks.CleanOldData(storageManager, runBackfill)
	})

	// Firehose consumer
	go utils.Recoverer(math.MaxInt, 1, func() {
		subscription := firehose.NewSubscription(
			"firehose",
			[]string{
				"jetstream1.us-east.bsky.network",
				"jetstream2.us-east.bsky.network",
				"jetstream1.us-west.bsky.network",
				"jetstream2.us-west.bsky.network",
			},
			storageManager,
		)
		subscription.Run()
	})

	if runBackfill {
		// Backfill
		numRepoWorkersStr := os.Getenv("BACKFILL_REPO_WORKERS")

		backfiller := backfill.NewBackfiller(
			"backfill",
			storageManager,
			utils.IntFromString(numRepoWorkersStr, 8),
		)
		go backfiller.Run()
	} else {
		// Statistics updater
		go utils.Recoverer(math.MaxInt, 1, func() {
			statisticsUpdater, err := tasks.NewStatisticsUpdater(storageManager)
			if err != nil {
				panic(err)
			}
			statisticsUpdater.Run()
		})
	}
}
