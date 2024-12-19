package models

import "time"

type User struct {
	ID             int32
	Did            string
	Handle         string
	FollowersCount int64
	FollowsCount   int64
	PostsCount     int64
	CreatedAt      time.Time
}
