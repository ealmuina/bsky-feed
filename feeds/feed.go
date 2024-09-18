package feeds

import (
	"bsky/cache"
	"bsky/db/sqlc"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
)

const TimelineInitPosts = 250
const CursorEOF = "eof"

type Feed struct {
	Name           string
	Queries        *db.Queries
	TimelinesCache *cache.TimelinesCache
	UsersCache     *cache.UsersCache
	algorithm      Algorithm
	acceptsPost    AlgorithmAcceptance
}

func getNewCursor() string {
	return fmt.Sprintf("0::%d::0", time.Now().Unix())
}

func feedPostToCachePost(post *Post) cache.Post {
	return cache.Post{
		Uri:         post.Uri,
		Reason:      post.Reason,
		CreatedAt:   post.CreatedAt,
		Cid:         post.Cid,
		AuthorDid:   post.AuthorDid,
		ReplyParent: post.ReplyParent,
		ReplyRoot:   post.ReplyRoot,
		Language:    post.Language,
	}
}

func NewFeed(
	name string,
	queries *db.Queries,
	timelinesCache *cache.TimelinesCache,
	usersCache *cache.UsersCache,
	algorithm Algorithm,
	acceptsPost AlgorithmAcceptance,
) *Feed {
	// Initialize timeline
	//posts := algorithm(
	//	QueryParams{Cursor: getNewCursor(), Limit: TimelineInitPosts},
	//	queries,
	//	context.Background(),
	//	time.Now(),
	//	"0",
	//)
	//for _, post := range posts {
	//	timelinesCache.AddPost(name, feedPostToCachePost(&post))
	//}

	// Build feeds
	return &Feed{
		Name:           name,
		Queries:        queries,
		TimelinesCache: timelinesCache,
		UsersCache:     usersCache,
		algorithm:      algorithm,
		acceptsPost:    acceptsPost,
	}
}

func (f *Feed) AddPost(post Post) {
	if ok, reason := f.acceptsPost(f, post); ok {
		post.Reason = reason
		f.TimelinesCache.AddPost(f.Name, feedPostToCachePost(&post))
	}
}

func (f *Feed) GetTimeline(params QueryParams) Response {
	ctx := context.Background()

	if params.Cursor == "" {
		params.Cursor = getNewCursor()
	} else if params.Cursor == CursorEOF {
		return Response{
			Cursor: CursorEOF,
			Posts:  make([]Post, 0),
		}
	}

	cursorParts := strings.Split(params.Cursor, "::")
	if len(cursorParts) != 3 {
		log.Errorf("Malformed cursor in %+v", params)
		return Response{}
	}

	timelineIndex, _ := strconv.ParseInt(cursorParts[0], 10, 64)
	createdAtInt, _ := strconv.ParseInt(cursorParts[1], 10, 64)

	createdAt := time.Unix(createdAtInt, 0)
	cid := cursorParts[2]

	// Attempt to hit cache first
	cachedPosts := f.TimelinesCache.GetTimeline(
		f.Name,
		timelineIndex,
		timelineIndex+params.Limit,
	)
	posts := make([]Post, len(cachedPosts))
	for i, cachedPost := range cachedPosts {
		posts[i].Uri = cachedPost.Uri
		posts[i].Reason = cachedPost.Reason
		posts[i].CreatedAt = cachedPost.CreatedAt
		posts[i].Cid = cachedPost.Cid
	}

	// Not found. Go to DB
	if int64(len(posts)) < params.Limit {
		posts = f.algorithm(params, f.Queries, ctx, createdAt, cid)

		// Add to cache
		for _, post := range posts {
			f.TimelinesCache.AddPost(f.Name, feedPostToCachePost(&post))
		}
	}

	cursor := CursorEOF
	if len(posts) > 0 {
		lastPost := posts[len(posts)-1]
		cursor = fmt.Sprintf(
			"%d::%d::%s",
			timelineIndex+int64(len(posts)),
			lastPost.CreatedAt.Unix(),
			lastPost.Cid,
		)
	}

	return Response{
		Cursor: cursor,
		Posts:  posts,
	}
}
