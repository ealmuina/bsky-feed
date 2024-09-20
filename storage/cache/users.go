package cache

import (
	"bsky/storage/models"
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

const UsersCacheRedisKey = "users"

type UsersCache struct {
	redisClient *redis.Client
}

func NewUsersCache(redisConnection *redis.Client) UsersCache {
	return UsersCache{
		redisClient: redisConnection,
	}
}

func (c *UsersCache) AddUser(user models.User) {
	bytes, err := json.Marshal(user)
	if err == nil {
		c.redisClient.HSet(
			context.Background(),
			UsersCacheRedisKey,
			user.Did,
			bytes,
		)
	}
}

func (c *UsersCache) DeleteUser(did string) {
	c.redisClient.HDel(context.Background(), UsersCacheRedisKey, did)
}

func (c *UsersCache) GetUser(did string) (bool, models.User) {
	val, err := c.redisClient.HGet(
		context.Background(),
		UsersCacheRedisKey,
		did,
	).Result()
	if err != nil {
		return false, models.User{}
	}

	var user models.User
	err = json.Unmarshal([]byte(val), &user)
	if err != nil {
		log.Errorf("Error unmarshalling post: %s", err)
		return false, models.User{}
	}
	return true, user
}
