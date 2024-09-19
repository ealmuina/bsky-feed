package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"time"
)

type Post struct {
	Uri    string
	Reason map[string]string
	Rank   float64
	//AuthorDid   string
	//ReplyParent string
	//ReplyRoot   string
	//Language    string
}

type TimelinesCache struct {
	redisClient *redis.Client
}

func NewTimelinesCache(options *redis.Options) *TimelinesCache {
	return &TimelinesCache{
		redisClient: redis.NewClient(options),
	}
}

func (c *TimelinesCache) AddPost(feedName string, post Post) {
	bytes, err := json.Marshal(post)
	if err == nil {
		c.redisClient.ZAdd(
			context.Background(),
			c.getRedisKey(feedName),
			redis.Z{
				Score:  post.Rank,
				Member: bytes,
			},
		)
	}
}

func (c *TimelinesCache) DeleteExpiredPosts(feedName string, expiration time.Time) {
	c.redisClient.ZRemRangeByScore(
		context.Background(),
		c.getRedisKey(feedName),
		"-inf",
		fmt.Sprintf("%d", expiration.Unix()),
	)
}

func (c *TimelinesCache) GetTimeline(feedName string, startScore float64, limit int64) []Post {
	members := c.redisClient.ZRevRangeByScore( // Retrieve in DESC order
		context.Background(),
		c.getRedisKey(feedName),
		&redis.ZRangeBy{
			Max:   fmt.Sprintf("%f", startScore),
			Count: limit,
		},
	)
	posts := make([]Post, len(members.Val()))
	for i, member := range members.Val() {
		err := json.Unmarshal([]byte(member), &posts[i])
		if err != nil {
			log.Errorf("Error unmarshalling post: %s", err)
		}
	}
	return posts
}

func (c *TimelinesCache) getRedisKey(feedName string) string {
	return fmt.Sprintf("feed__%s", feedName)
}
