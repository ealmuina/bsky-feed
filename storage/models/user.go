package models

type User struct {
	Did              string
	Handle           string
	FollowersCount   int64
	FollowsCount     int64
	PostsCount       int64
	EngagementFactor float64
}
