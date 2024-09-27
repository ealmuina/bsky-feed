package cache

import (
	"bsky/storage/models"
	"context"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"strconv"
	"time"
)

const PostAuthorCacheRedisKey = "posts_author"
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

func (c *PostsCache) AddPost(post models.Post) {
	ctx := context.Background()
	c.redisClient.HSet(ctx, PostAuthorCacheRedisKey, post.Uri, post.AuthorDid)
	c.redisClient.HExpire(ctx, PostAuthorCacheRedisKey, c.expiration, post.Uri)
}

func (c *PostsCache) AddInteraction(postUri string) {
	ctx := context.Background()
	c.redisClient.HIncrBy(ctx, PostInteractionsCountCacheRedisKey, postUri, 1)
	c.redisClient.HExpire(ctx, PostInteractionsCountCacheRedisKey, c.expiration, postUri)
}

func (c *PostsCache) DeleteInteraction(postUri string) {
	ctx := context.Background()
	c.redisClient.HIncrBy(ctx, PostInteractionsCountCacheRedisKey, postUri, -1)
}

func (c *PostsCache) DeletePost(uri string) bool {
	result := c.redisClient.HDel(context.Background(), PostAuthorCacheRedisKey, uri)
	c.redisClient.HDel(context.Background(), PostInteractionsCountCacheRedisKey, uri)
	return result.Val() != 0
}

func (c *PostsCache) DeletePosts(uris []string) {
	c.redisClient.HDel(context.Background(), PostAuthorCacheRedisKey, uris...)
	c.redisClient.HDel(context.Background(), PostInteractionsCountCacheRedisKey, uris...)
}

func (c *PostsCache) GetPostAuthor(uri string) (string, bool) {
	authorDid, err := c.redisClient.HGet(
		context.Background(),
		PostAuthorCacheRedisKey,
		uri,
	).Result()
	if err != nil {
		return "", false
	}
	return authorDid, true
}

func (c *PostsCache) GetPostInteractions(uri string) int64 {
	interactionsCountStr, err := c.redisClient.HGet(
		context.Background(),
		PostInteractionsCountCacheRedisKey,
		uri,
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
