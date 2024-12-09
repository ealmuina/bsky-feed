package models

import "time"

type Follow struct {
	UriKey    string    `bson:"uri_key"`
	AuthorID  string    `bson:"author_id"`
	SubjectID string    `bson:"subject_id"`
	CreatedAt time.Time `bson:"created_at"`
}
