package cache

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

const UsersCacheRedisKey = "users"

type User struct {
	Did              string  `json:"did"`
	FollowersCount   int64   `json:"followers_count"`
	EngagementFactor float64 `json:"engagement_factor"`
}

type UsersCache struct {
	redisClient *redis.Client
}

func NewUsersCache(options *redis.Options) *UsersCache {
	return &UsersCache{
		redisClient: redis.NewClient(options),
	}
}

func (c *UsersCache) AddUser(user User) {
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

func (c *UsersCache) GetUser(did string) (bool, User) {
	val, err := c.redisClient.HGet(
		context.Background(),
		UsersCacheRedisKey,
		did,
	).Result()
	if err != nil {
		return false, User{}
	}

	var user User
	err = json.Unmarshal([]byte(val), &user)
	if err != nil {
		log.Errorf("Error unmarshalling post: %s", err)
		return false, User{}
	}
	return true, user
}
