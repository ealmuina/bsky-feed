package cache

import (
	"context"
	"github.com/redis/go-redis/v9"
	"math"
	"time"
)

const UsersFollowersCountCacheRedisKey = "users_followers_count"
const UsersFollowsCountCacheRedisKey = "users_follows_count"
const UsersPostsCountCacheRedisKey = "users_posts_count"
const UsersInteractionsCountCacheRedisKey = "users_interactions_count"

type UserStatistics struct {
	Did               string
	FollowersCount    int64
	FollowsCount      int64
	PostsCount        int64
	InteractionsCount int64
}

func (s *UserStatistics) GetEngagementFactor() float64 {
	interactionsCount := float64(s.InteractionsCount)
	postsCount := float64(s.PostsCount)
	followersCount := float64(s.FollowersCount)

	if postsCount == 0 || followersCount < 10 {
		return -1
	}
	return ((interactionsCount / postsCount) * 100.0 / followersCount) / (5 / math.Log(followersCount))
}

type UsersCache struct {
	redisClient *redis.Client
	expiration  time.Duration
}

func NewUsersCache(redisConnection *redis.Client, expiration time.Duration) UsersCache {
	return UsersCache{
		redisClient: redisConnection,
		expiration:  expiration,
	}
}

func (c *UsersCache) DeleteUser(did string) {
	ctx := context.Background()
	c.redisClient.HDel(ctx, UsersFollowersCountCacheRedisKey, did)
	c.redisClient.HDel(ctx, UsersFollowsCountCacheRedisKey, did)
	c.redisClient.HDel(ctx, UsersPostsCountCacheRedisKey, did)
	c.redisClient.HDel(ctx, UsersInteractionsCountCacheRedisKey, did)
}

func (c *UsersCache) DeleteUsers(dids []string) {
	ctx := context.Background()
	c.redisClient.HDel(ctx, UsersFollowersCountCacheRedisKey, dids...)
	c.redisClient.HDel(ctx, UsersFollowsCountCacheRedisKey, dids...)
	c.redisClient.HDel(ctx, UsersPostsCountCacheRedisKey, dids...)
	c.redisClient.HDel(ctx, UsersInteractionsCountCacheRedisKey, dids...)
}

func (c *UsersCache) GetUserStatistics(did string) UserStatistics {
	ctx := context.Background()

	followersCount, _ := c.redisClient.HGet(ctx, UsersFollowersCountCacheRedisKey, did).Int64()
	followsCount, _ := c.redisClient.HGet(ctx, UsersFollowsCountCacheRedisKey, did).Int64()
	postsCount, _ := c.redisClient.HGet(ctx, UsersPostsCountCacheRedisKey, did).Int64()
	interactionsCount, _ := c.redisClient.HGet(ctx, UsersInteractionsCountCacheRedisKey, did).Int64()

	return UserStatistics{
		Did:               did,
		FollowersCount:    followersCount,
		FollowsCount:      followsCount,
		PostsCount:        postsCount,
		InteractionsCount: interactionsCount,
	}
}

func (c *UsersCache) UpdateUserStatistics(
	did string,
	followsDelta int64,
	followersDelta int64,
	postsDelta int64,
	interactionsDelta int64,
) {
	ctx := context.Background()

	for redisKey, delta := range map[string]int64{
		UsersFollowersCountCacheRedisKey:    followersDelta,
		UsersFollowsCountCacheRedisKey:      followsDelta,
		UsersPostsCountCacheRedisKey:        postsDelta,
		UsersInteractionsCountCacheRedisKey: interactionsDelta,
	} {
		if delta != 0 {
			c.redisClient.HIncrBy(ctx, redisKey, did, delta)
			c.redisClient.HExpire(ctx, redisKey, c.expiration, did)
		}
	}
}
