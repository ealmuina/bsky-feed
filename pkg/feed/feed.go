package feed

import (
	"bsky/pkg/auth"
	"bsky/pkg/models"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"strconv"
	"strings"
	"time"
)

type Algorithm func(*auth.AuthConfig, *gorm.DB, QueryParams) (string, []SkeletonItem)

type QueryParams struct {
	Limit    int
	Cursor   *string
	Language *string
}

type ReasonRepost struct {
	Type   string `json:"$type"`  // must always be "app.bsky.feed.defs#skeletonReasonRepost"
	Repost string `json:"repost"` // repost URI
}

type SkeletonItem struct {
	Post string `json:"post"`
	//Reason *ReasonRepost `json:"reason"`
}

func GetFeed(db *gorm.DB, params QueryParams, filter func(*gorm.DB) *gorm.DB) (string, []string) {
	query := db.Order(
		"indexed_at desc",
	).Order(
		"cid desc",
	).Limit(
		params.Limit,
	)

	if params.Cursor != nil {
		s := strings.Split(*params.Cursor, "::")

		if len(s) != 2 {
			log.Errorf("Malformed cursor in %+v", params)
		} else {
			indexedAtInt, _ := strconv.ParseInt(s[0], 10, 64)
			indexedAt := time.Unix(indexedAtInt, 0)
			cid := s[1]

			query = query.Where(
				query.Where(
					"indexed_at < ?", indexedAt,
				).Or(
					"indexed_at = ? AND cid < ?", indexedAt, cid,
				),
			)
		}
	}

	if params.Language != nil {
		query = query.Where("language = ?", params.Language)
	}

	query = filter(query)

	var posts []models.Post
	query.Find(&posts)

	var feed []string
	var cursor string

	if len(posts) > 0 {
		for _, post := range posts {
			feed = append(feed, post.Uri)
		}
		lastPost := posts[len(posts)-1]
		cursor = fmt.Sprintf(
			"%v::%v",
			lastPost.IndexedAt.Unix(),
			lastPost.Cid,
		)
	}

	return cursor, feed
}
