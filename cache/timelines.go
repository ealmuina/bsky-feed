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
	Cid         string // Keep first as Redis sorts in lexicographic order when score is the same
	Uri         string
	Reason      map[string]string
	CreatedAt   time.Time
	AuthorDid   string
	ReplyParent string
	ReplyRoot   string
	Language    string
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
				Score:  float64(-post.CreatedAt.Unix()), // Sort DESC
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

func (c *TimelinesCache) GetTimeline(feedName string, startIndex int64, endIndex int64) []Post {
	members := c.redisClient.ZRange(
		context.Background(),
		c.getRedisKey(feedName),
		startIndex,
		endIndex,
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
