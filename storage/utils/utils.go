package utils

import (
	"bsky/storage/db/models"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
)

type PostContent struct {
	Post  models.PostsStruct
	Text  string
	Embed *appbsky.FeedPost_Embed
}
