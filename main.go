package main

import (
	"bsky/firehose"
	"bsky/monitoring"
	"bsky/server"
	"bsky/storage"
	"bsky/tasks"
	"bsky/utils"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/scylladb/gocqlx/v3"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"math"
	"net/url"
	"os"
	"time"
)

func init() {
	log.SetLevel(log.WarnLevel)

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
	dbHost := os.Getenv("DB_HOST")
	cluster := createCluster("bsky", dbHost)
	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		log.Fatal("unable to connect to scylla", zap.Error(err))
	}
	defer session.Close()

	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisClient := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", redisHost, redisPort),
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	storageManager := storage.NewManager(&session, redisClient)

	// Run background tasks
	runBackgroundTasks(storageManager)

	// Run server
	s := server.NewServer(storageManager)
	s.Run()
}

func createCluster(keyspace string, host string) *gocql.ClusterConfig {
	retryPolicy := &gocql.ExponentialBackoffRetryPolicy{
		Min:        time.Second,
		Max:        10 * time.Second,
		NumRetries: 5,
	}
	cluster := gocql.NewCluster(host)
	cluster.Keyspace = keyspace
	cluster.Timeout = time.Minute
	cluster.RetryPolicy = retryPolicy
	return cluster
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
