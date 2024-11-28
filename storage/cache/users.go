package cache

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"math"
	"strconv"
	"time"
)

const UsersFollowersCountRedisKey = "users_followers_count"
const UsersFollowsCountRedisKey = "users_follows_count"
const UsersPostsCountRedisKey = "users_posts_count"
const UsersInteractionsCountRedisKey = "users_interactions_count"
const UserIdToDidRedisKey = "users_id_to_did"
const UserDidToIdRedisKey = "users_did_to_id"

type UserStatistics struct {
	ID                int32
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

func (c *UsersCache) AddUser(id int32, did string) {
	idStr := fmt.Sprintf("%d", id)
	c.hSetWithExpiration(UserIdToDidRedisKey, idStr, did)
	c.hSetWithExpiration(UserDidToIdRedisKey, did, idStr)
}

func (c *UsersCache) DeleteUser(id int32) {
	ctx := context.Background()
	idStr := fmt.Sprintf("%d", id)
	c.redisClient.HDel(ctx, UsersFollowersCountRedisKey, idStr)
	c.redisClient.HDel(ctx, UsersFollowsCountRedisKey, idStr)
	c.redisClient.HDel(ctx, UsersPostsCountRedisKey, idStr)
	c.redisClient.HDel(ctx, UsersInteractionsCountRedisKey, idStr)
}

func (c *UsersCache) DeleteUsers(dids []string) {
	ctx := context.Background()
	c.redisClient.HDel(ctx, UsersFollowersCountRedisKey, dids...)
	c.redisClient.HDel(ctx, UsersFollowsCountRedisKey, dids...)
	c.redisClient.HDel(ctx, UsersPostsCountRedisKey, dids...)
	c.redisClient.HDel(ctx, UsersInteractionsCountRedisKey, dids...)
}

func (c *UsersCache) GetUserStatistics(id int32) UserStatistics {
	ctx := context.Background()
	idStr := fmt.Sprintf("%d", id)

	followersCount, _ := c.redisClient.HGet(ctx, UsersFollowersCountRedisKey, idStr).Int64()
	followsCount, _ := c.redisClient.HGet(ctx, UsersFollowsCountRedisKey, idStr).Int64()
	postsCount, _ := c.redisClient.HGet(ctx, UsersPostsCountRedisKey, idStr).Int64()
	interactionsCount, _ := c.redisClient.HGet(ctx, UsersInteractionsCountRedisKey, idStr).Int64()

	return UserStatistics{
		ID:                id,
		FollowersCount:    followersCount,
		FollowsCount:      followsCount,
		PostsCount:        postsCount,
		InteractionsCount: interactionsCount,
	}
}

func (c *UsersCache) RequiresReload() bool {
	return c.redisClient.Exists(context.Background(), UsersFollowersCountRedisKey).Val() > 0
}

func (c *UsersCache) SetUserFollows(id int32, followersCount int64, followsCount int64) {
	idStr := fmt.Sprintf("%d", id)
	c.hSetWithExpiration(UsersFollowersCountRedisKey, idStr, strconv.Itoa(int(followersCount)))
	c.hSetWithExpiration(UsersFollowsCountRedisKey, idStr, strconv.Itoa(int(followsCount)))
}

func (c *UsersCache) UpdateUserStatistics(
	id int32,
	followsDelta int64,
	followersDelta int64,
	postsDelta int64,
	interactionsDelta int64,
) {
	ctx := context.Background()
	idStr := fmt.Sprintf("%d", id)

	for redisKey, delta := range map[string]int64{
		UsersFollowersCountRedisKey:    followersDelta,
		UsersFollowsCountRedisKey:      followsDelta,
		UsersPostsCountRedisKey:        postsDelta,
		UsersInteractionsCountRedisKey: interactionsDelta,
	} {
		if delta != 0 {
			c.redisClient.HIncrBy(ctx, redisKey, idStr, delta)
			c.redisClient.HExpire(ctx, redisKey, c.expiration, idStr)
		}
	}
}

func (c *UsersCache) UserIdToDid(id int32) (string, bool) {
	did, err := c.redisClient.HGet(context.Background(), UserIdToDidRedisKey, strconv.Itoa(int(id))).Result()
	if err != nil {
		return "", false
	}
	return did, true
}

func (c *UsersCache) UserDidToId(did string) (int32, bool) {
	id, err := c.redisClient.HGet(context.Background(), UserDidToIdRedisKey, did).Int()
	if err != nil {
		return 0, false
	}
	return int32(int64(id)), true
}

func (c *UsersCache) hSetWithExpiration(redisKey, key, value string) {
	ctx := context.Background()
	c.redisClient.HSet(ctx, redisKey, key, value)
	c.redisClient.HExpire(ctx, redisKey, c.expiration, key)
}
