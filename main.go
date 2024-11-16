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
	cluster := createCluster(gocql.Quorum, "catalog", "db")
	session, err := gocql.NewSession(*cluster)
	if err != nil {
		log.Fatal("unable to connect to scylla", zap.Error(err))
	}
	sessionx := gocqlx.NewSession(session)
	defer sessionx.Close()

	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisClient := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", redisHost, redisPort),
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	storageManager := storage.NewManager(&sessionx, redisClient)

	// Run background tasks
	runBackgroundTasks(storageManager)

	// Run server
	s := server.NewServer(storageManager)
	s.Run()
}

func createCluster(consistency gocql.Consistency, keyspace string, hosts ...string) *gocql.ClusterConfig {
	retryPolicy := &gocql.ExponentialBackoffRetryPolicy{
		Min:        time.Second,
		Max:        10 * time.Second,
		NumRetries: 5,
	}
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Timeout = 5 * time.Second
	cluster.RetryPolicy = retryPolicy
	cluster.Consistency = consistency
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
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
