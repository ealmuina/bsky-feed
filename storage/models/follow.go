package models

import "time"

type Follow struct {
	UriKey    string
	AuthorID  int32
	SubjectID int32
	IndexedAt time.Time
	CreatedAt time.Time
}
