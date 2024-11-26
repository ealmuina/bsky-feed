package models

import "time"

type Follow struct {
	UriKey    string
	AuthorID  int32
	SubjectID int32
	CreatedAt time.Time
}
