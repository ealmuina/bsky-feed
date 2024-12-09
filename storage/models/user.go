package models

import "go.mongodb.org/mongo-driver/bson/primitive"

type User struct {
	Id             primitive.ObjectID `bson:"_id"`
	Did            string             `bson:"did"`
	Handle         string             `bson:"handle"`
	FollowersCount int64              `bson:"followers_count"`
	FollowsCount   int64              `bson:"follows_count"`
	PostsCount     int64              `bson:"posts_count"`
	LastUpdated    string             `bson:"last_updated,omitempty"`
}
