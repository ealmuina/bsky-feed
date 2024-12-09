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
	Id                string
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

func (c *UsersCache) AddUser(id string, did string) {
	c.hSetWithExpiration(UserIdToDidRedisKey, id, did)
	c.hSetWithExpiration(UserDidToIdRedisKey, did, id)
}

func (c *UsersCache) DeleteUser(id string) {
	ctx := context.Background()
	c.redisClient.HDel(ctx, UsersFollowersCountRedisKey, id)
	c.redisClient.HDel(ctx, UsersFollowsCountRedisKey, id)
	c.redisClient.HDel(ctx, UsersPostsCountRedisKey, id)
	c.redisClient.HDel(ctx, UsersInteractionsCountRedisKey, id)
}

func (c *UsersCache) DeleteUsers(dids []string) {
	ctx := context.Background()
	c.redisClient.HDel(ctx, UsersFollowersCountRedisKey, dids...)
	c.redisClient.HDel(ctx, UsersFollowsCountRedisKey, dids...)
	c.redisClient.HDel(ctx, UsersPostsCountRedisKey, dids...)
	c.redisClient.HDel(ctx, UsersInteractionsCountRedisKey, dids...)
}

func (c *UsersCache) GetUserStatistics(id string) UserStatistics {
	ctx := context.Background()

	followersCount, _ := c.redisClient.HGet(ctx, UsersFollowersCountRedisKey, id).Int64()
	followsCount, _ := c.redisClient.HGet(ctx, UsersFollowsCountRedisKey, id).Int64()
	postsCount, _ := c.redisClient.HGet(ctx, UsersPostsCountRedisKey, id).Int64()
	interactionsCount, _ := c.redisClient.HGet(ctx, UsersInteractionsCountRedisKey, id).Int64()

	return UserStatistics{
		Id:                id,
		FollowersCount:    followersCount,
		FollowsCount:      followsCount,
		PostsCount:        postsCount,
		InteractionsCount: interactionsCount,
	}
}

func (c *UsersCache) RequiresReload() bool {
	return c.redisClient.Exists(context.Background(), UsersFollowersCountRedisKey).Val() > 0
}

func (c *UsersCache) SetUserFollows(id string, followersCount int64, followsCount int64) {
	idStr := fmt.Sprintf("%d", id)
	c.hSetWithExpiration(UsersFollowersCountRedisKey, idStr, strconv.Itoa(int(followersCount)))
	c.hSetWithExpiration(UsersFollowsCountRedisKey, idStr, strconv.Itoa(int(followsCount)))
}

func (c *UsersCache) UpdateUserStatistics(
	id string,
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

func (c *UsersCache) UserDidToId(did string) (string, bool) {
	id, err := c.redisClient.HGet(context.Background(), UserDidToIdRedisKey, did).Result()
	if err != nil {
		return "", false
	}
	return id, true
}

func (c *UsersCache) hSetWithExpiration(redisKey, key, value string) {
	ctx := context.Background()
	c.redisClient.HSet(ctx, redisKey, key, value)
	c.redisClient.HExpire(ctx, redisKey, c.expiration, key)
}
