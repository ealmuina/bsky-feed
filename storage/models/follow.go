package models

import "time"

type Follow struct {
	Uri        string
	AuthorDid  string
	SubjectDid string
	IndexedAt  time.Time
	CreatedAt  time.Time
}
