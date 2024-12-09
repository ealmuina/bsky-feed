package cache

import (
	"bsky/storage/models"
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"strconv"
	"time"
)

const PostInteractionsCountRedisKey = "posts_interactions_count"
const PostAuthorIdRedisKey = "posts_author_id"
const PostIdRedisKey = "posts_id"

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

func (c *PostsCache) AddInteraction(postId string) {
	ctx := context.Background()
	c.redisClient.HIncrBy(ctx, PostInteractionsCountRedisKey, postId, 1)
	c.redisClient.HExpire(ctx, PostInteractionsCountRedisKey, c.expiration, postId)
}

func (c *PostsCache) AddPost(post models.Post) {
	idStr := post.Id.Hex()
	c.hSetWithExpiration(PostIdRedisKey, post.Uri(), idStr)
	c.hSetWithExpiration(PostAuthorIdRedisKey, idStr, post.AuthorId)
}

func (c *PostsCache) DeleteInteraction(postId string) bool {
	ctx := context.Background()
	result, err := c.redisClient.HIncrBy(ctx, PostInteractionsCountRedisKey, postId, -1).Result()
	if err != nil {
		return false
	}
	return result > 0
}

func (c *PostsCache) DeletePost(id string) {
	ctx := context.Background()
	c.redisClient.HDel(ctx, PostInteractionsCountRedisKey, id)
	c.redisClient.HDel(ctx, PostAuthorIdRedisKey, id)
}

func (c *PostsCache) DeletePosts(id []string) {
	ctx := context.Background()
	c.redisClient.HDel(ctx, PostInteractionsCountRedisKey, id...)
	c.redisClient.HDel(ctx, PostAuthorIdRedisKey, id...)
}

func (c *PostsCache) GetPostAuthorId(id string) (string, bool) {
	ctx := context.Background()
	authorId, err := c.redisClient.HGet(ctx, PostAuthorIdRedisKey, id).Result()
	if err != nil {
		return "", false
	}
	return authorId, true
}

func (c *PostsCache) GetPostId(authorId string, uriKey string) (string, bool) {
	ctx := context.Background()
	uriStr := fmt.Sprintf("%d/%s", authorId, uriKey)
	id, err := c.redisClient.HGet(ctx, PostIdRedisKey, uriStr).Result()
	if err != nil {
		return "", false
	}
	return id, true
}

func (c *PostsCache) GetPostInteractions(id string) int64 {
	interactionsCountStr, err := c.redisClient.HGet(
		context.Background(),
		PostInteractionsCountRedisKey,
		id,
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

func (c *PostsCache) hSetWithExpiration(redisKey, key, value string) {
	ctx := context.Background()
	c.redisClient.HSet(ctx, redisKey, key, value)
	c.redisClient.HExpire(ctx, redisKey, c.expiration, key)
}
