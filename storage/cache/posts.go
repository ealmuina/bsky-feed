package cache

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"strconv"
	"time"
)

const PostInteractionsCountCacheRedisKey = "posts_interactions_count"

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

func (c *PostsCache) AddInteraction(authorId int32, uriKey string) {
	ctx := context.Background()
	idStr := fmt.Sprintf("%d/%s", authorId, uriKey)
	c.redisClient.HIncrBy(ctx, PostInteractionsCountCacheRedisKey, idStr, 1)
	c.redisClient.HExpire(ctx, PostInteractionsCountCacheRedisKey, c.expiration, idStr)
}

func (c *PostsCache) DeleteInteraction(authorId int32, uriKey string) {
	ctx := context.Background()
	idStr := fmt.Sprintf("%d/%s", authorId, uriKey)
	c.redisClient.HIncrBy(ctx, PostInteractionsCountCacheRedisKey, idStr, -1)
}

func (c *PostsCache) DeletePost(id int32) bool {
	ctx := context.Background()
	idStr := fmt.Sprintf("%d", id)
	result := c.redisClient.HDel(ctx, PostInteractionsCountCacheRedisKey, idStr)
	return result.Val() != 0
}

func (c *PostsCache) DeletePosts(id []int32) {
	ctx := context.Background()

	idStr := make([]string, len(id))
	for i, v := range id {
		idStr[i] = fmt.Sprintf("%d", v)
	}

	c.redisClient.HDel(ctx, PostInteractionsCountCacheRedisKey, idStr...)
}

func (c *PostsCache) GetPostInteractions(id int32) int32 {
	idStr := fmt.Sprintf("%d", id)
	interactionsCountStr, err := c.redisClient.HGet(
		context.Background(),
		PostInteractionsCountCacheRedisKey,
		idStr,
	).Result()
	if err != nil {
		return 0
	}
	interactionsCount, err := strconv.Atoi(interactionsCountStr)
	if err != nil {
		log.Errorf("Could not convert value to int: %v", err)
	}
	return int32(interactionsCount)
}
