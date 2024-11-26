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

func (c *PostsCache) AddInteraction(postId int64) {
	ctx := context.Background()
	idStr := strconv.FormatInt(postId, 10)
	c.redisClient.HIncrBy(ctx, PostInteractionsCountRedisKey, idStr, 1)
	c.redisClient.HExpire(ctx, PostInteractionsCountRedisKey, c.expiration, idStr)
}

func (c *PostsCache) AddPost(post models.Post) {
	idStr := strconv.FormatInt(post.ID, 10)
	authorIdStr := strconv.Itoa(int(post.AuthorId))
	uriStr := fmt.Sprintf("%d/%s", post.AuthorId, post.UriKey)

	c.hSetWithExpiration(PostIdRedisKey, uriStr, idStr)
	c.hSetWithExpiration(PostAuthorIdRedisKey, idStr, authorIdStr)
}

func (c *PostsCache) DeleteInteraction(postId int64) {
	ctx := context.Background()
	idStr := strconv.FormatInt(postId, 10)
	c.redisClient.HIncrBy(ctx, PostInteractionsCountRedisKey, idStr, -1)
}

func (c *PostsCache) DeletePost(id int64) {
	ctx := context.Background()
	idStr := strconv.FormatInt(id, 10)

	c.redisClient.HDel(ctx, PostInteractionsCountRedisKey, idStr)
	c.redisClient.HDel(ctx, PostAuthorIdRedisKey, idStr)
}

func (c *PostsCache) DeletePosts(id []int64) {
	ctx := context.Background()

	idStr := make([]string, len(id))
	for i, v := range id {
		idStr[i] = fmt.Sprintf("%d", v)
	}

	c.redisClient.HDel(ctx, PostInteractionsCountRedisKey, idStr...)
	c.redisClient.HDel(ctx, PostAuthorIdRedisKey, idStr...)
}

func (c *PostsCache) GetPostAuthorId(id int64) (int32, bool) {
	ctx := context.Background()
	idStr := strconv.FormatInt(id, 10)

	authorIdStr, err := c.redisClient.HGet(ctx, PostAuthorIdRedisKey, idStr).Result()
	if err != nil {
		return 0, false
	}
	authorId, err := strconv.Atoi(authorIdStr)
	if err != nil {
		return 0, false
	}
	return int32(authorId), true
}

func (c *PostsCache) GetPostId(authorId int32, uriKey string) (int64, bool) {
	ctx := context.Background()
	uriStr := fmt.Sprintf("%d/%s", authorId, uriKey)

	idStr, err := c.redisClient.HGet(ctx, PostIdRedisKey, uriStr).Result()
	if err != nil {
		return 0, false
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
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

func (c *PostsCache) hSetWithExpiration(redisKey, key, value string) {
	ctx := context.Background()
	c.redisClient.HSet(ctx, redisKey, key, value)
	c.redisClient.HExpire(ctx, redisKey, c.expiration, key)
}
