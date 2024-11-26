package cache

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"strconv"
	"time"
)

const PostInteractionsCountRedisKey = "posts_interactions_count"

type PostsCache struct {
	redisClient *redis.Client
	expiration  time.Duration
}

func NewPostsCache(redisConnection *redis.Client, expiration time.Duration) PostsCache {
	return PostsCache{
		redisClient: redisConnection,
		expiration:  expiration,
	}
}

func (c *PostsCache) AddInteraction(postId int64) {
	ctx := context.Background()
	idStr := strconv.FormatInt(postId, 10)
	c.redisClient.HIncrBy(ctx, PostInteractionsCountRedisKey, idStr, 1)
	c.redisClient.HExpire(ctx, PostInteractionsCountRedisKey, c.expiration, idStr)
}

func (c *PostsCache) DeleteInteraction(postId int64) {
	ctx := context.Background()
	idStr := strconv.FormatInt(postId, 10)
	c.redisClient.HIncrBy(ctx, PostInteractionsCountRedisKey, idStr, -1)
}

func (c *PostsCache) DeletePost(id int64) bool {
	ctx := context.Background()
	idStr := strconv.FormatInt(id, 10)
	result := c.redisClient.HDel(ctx, PostInteractionsCountRedisKey, idStr)
	return result.Val() != 0
}

func (c *PostsCache) DeletePosts(id []int64) {
	ctx := context.Background()

	idStr := make([]string, len(id))
	for i, v := range id {
		idStr[i] = fmt.Sprintf("%d", v)
	}

	c.redisClient.HDel(ctx, PostInteractionsCountRedisKey, idStr...)
}

func (c *PostsCache) GetPostInteractions(id int64) int64 {
	idStr := strconv.FormatInt(id, 10)
	interactionsCountStr, err := c.redisClient.HGet(
		context.Background(),
		PostInteractionsCountRedisKey,
		idStr,
	).Result()
	if err != nil {
		return 0
	}
	interactionsCount, err := strconv.Atoi(interactionsCountStr)
	if err != nil {
		log.Errorf("Could not convert value to int: %v", err)
	}
	return int64(interactionsCount)
}
