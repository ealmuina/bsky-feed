package cache

import (
	"bsky/storage/models"
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"time"
)

const UsersCacheRedisKey = "users"

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

func (c *UsersCache) AddUser(user models.User) {
	bytes, err := json.Marshal(user)
	if err == nil {
		ctx := context.Background()
		c.redisClient.HSet(ctx, UsersCacheRedisKey, user.Did, bytes)
		c.redisClient.HExpire(ctx, UsersCacheRedisKey, c.expiration, user.Did)
	}
}

func (c *UsersCache) DeleteUser(did string) {
	c.redisClient.HDel(context.Background(), UsersCacheRedisKey, did)
}

func (c *UsersCache) DeleteUsers(dids []string) {
	c.redisClient.HDel(context.Background(), UsersCacheRedisKey, dids...)
}

func (c *UsersCache) GetUser(did string) (models.User, bool) {
	val, err := c.redisClient.HGet(
		context.Background(),
		UsersCacheRedisKey,
		did,
	).Result()
	if err != nil {
		return models.User{}, false
	}

	var user models.User
	err = json.Unmarshal([]byte(val), &user)
	if err != nil {
		log.Errorf("Error unmarshalling post: %s", err)
		return models.User{}, false
	}
	return user, false
}

func (c *UsersCache) UpdateUserCounts(did string, followsDelta int64, followersDelta int64, postsDelta int64) {
	// TODO Deal with race conditions... (HINCRBY?)
	user, ok := c.GetUser(did)
	if !ok {
		return
	}
	user.FollowsCount += followsDelta
	user.FollowersCount += followersDelta
	user.PostsCount += postsDelta
	c.AddUser(user)
}
