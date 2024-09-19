package feeds

import (
	"bsky/cache"
	"bsky/db/sqlc"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
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
	return fmt.Sprintf("%d", time.Now().Unix())
}

func feedPostToCachePost(post *Post) cache.Post {
	return cache.Post{
		Uri:    post.Uri,
		Reason: post.Reason,
		Rank:   post.Rank,
		//CreatedAt:   post.CreatedAt,
		//Cid:         post.Cid,
		//AuthorDid:   post.AuthorDid,
		//ReplyParent: post.ReplyParent,
		//ReplyRoot:   post.ReplyRoot,
		//Language:    post.Language,
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

	cursorRank, err := strconv.ParseFloat(params.Cursor, 64)
	if err != nil {
		log.Errorf("Malformed cursor in %+v", params)
		return Response{}
	}

	// Attempt to hit cache first
	cachedPosts := f.TimelinesCache.GetTimeline(
		f.Name,
		cursorRank,
		params.Limit,
	)
	posts := make([]Post, len(cachedPosts))
	for i, cachedPost := range cachedPosts {
		posts[i].Uri = cachedPost.Uri
		posts[i].Reason = cachedPost.Reason
		//posts[i].CreatedAt = cachedPost.CreatedAt
		posts[i].Rank = cachedPost.Rank
	}

	// Not found. Go to DB
	if int64(len(posts)) < params.Limit {
		posts = f.algorithm(params, f.Queries, ctx, cursorRank)

		// Add to cache
		for _, post := range posts {
			f.TimelinesCache.AddPost(f.Name, feedPostToCachePost(&post))
		}
	}

	cursor := CursorEOF
	if len(posts) > 0 {
		lastPost := posts[len(posts)-1]
		cursor = fmt.Sprintf("%f", lastPost.Rank)
	}

	return Response{
		Cursor: cursor,
		Posts:  posts,
	}
}
