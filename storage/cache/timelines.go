package cache

import (
	"bsky/storage/models"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

const redisTimeout = 2 * time.Second

type Timeline struct {
	name        string
	redisClient *redis.Client
}

func NewTimeline(name string, redisClient *redis.Client) Timeline {
	return Timeline{
		name:        name,
		redisClient: redisClient,
	}
}

func (t *Timeline) AddPost(entry models.TimelineEntry) {
	bytes, err := json.Marshal(entry)
	if err == nil {
		t.redisClient.ZAddNX(
			context.Background(),
			t.getRedisKey(),
			redis.Z{
				Score:  entry.Rank,
				Member: bytes,
			},
		)
	}
}

func (t *Timeline) DeleteExpiredPosts(expiration time.Time) {
	t.redisClient.ZRemRangeByScore(
		context.Background(),
		t.getRedisKey(),
		"-inf",
		fmt.Sprintf("%d", expiration.Unix()),
	)
}

func (t *Timeline) GetPosts(maxScore float64, limit int64) []models.TimelineEntry {
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeout)
	defer cancel()
	members := t.redisClient.ZRevRangeByScore( // Retrieve in DESC order
		ctx,
		t.getRedisKey(),
		&redis.ZRangeBy{
			Max:   fmt.Sprintf("%f", maxScore),
			Count: limit,
		},
	)
	entries := make([]models.TimelineEntry, len(members.Val()))
	for i, member := range members.Val() {
		err := json.Unmarshal([]byte(member), &entries[i])
		if err != nil {
			log.Errorf("Error unmarshalling post: %s", err)
		}
	}
	return entries
}

func (t *Timeline) getRedisKey() string {
	return fmt.Sprintf("feed__%s", t.name)
}
