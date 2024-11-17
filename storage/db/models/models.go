// Code generated by "gocqlx/cmd/schemagen"; DO NOT EDIT.

package models

import (
	"github.com/scylladb/gocqlx/v3/table"
	"time"
)

// Table models.
var (
	Follows = table.New(table.Metadata{
		Name: "follows",
		Columns: []string{
			"author_did",
			"created_at",
			"indexed_at",
			"subject_did",
			"uri",
		},
		PartKey: []string{
			"uri",
		},
		SortKey: []string{
			"subject_did",
			"created_at",
		},
	})

	Interactions = table.New(table.Metadata{
		Name: "interactions",
		Columns: []string{
			"author_did",
			"created_at",
			"indexed_at",
			"kind",
			"post_uri",
			"uri",
		},
		PartKey: []string{
			"uri",
		},
		SortKey: []string{
			"post_uri",
			"created_at",
		},
	})

	Posts = table.New(table.Metadata{
		Name: "posts",
		Columns: []string{
			"author_did",
			"created_at",
			"indexed_at",
			"language",
			"rank",
			"reply_parent",
			"reply_root",
			"uri",
		},
		PartKey: []string{
			"uri",
		},
		SortKey: []string{
			"language",
			"rank",
		},
	})

	SubscriptionState = table.New(table.Metadata{
		Name: "subscription_state",
		Columns: []string{
			"cursor",
			"id",
			"service",
		},
		PartKey: []string{
			"id",
		},
		SortKey: []string{},
	})

	Users = table.New(table.Metadata{
		Name: "users",
		Columns: []string{
			"did",
			"followers_count",
			"follows_count",
			"handle",
			"indexed_at",
			"last_update",
			"posts_count",
		},
		PartKey: []string{
			"did",
		},
		SortKey: []string{
			"last_update",
		},
	})
)

type FollowsStruct struct {
	AuthorDid  string
	CreatedAt  time.Time
	IndexedAt  time.Time
	SubjectDid string
	Uri        string
}
type InteractionsStruct struct {
	AuthorDid string
	CreatedAt time.Time
	IndexedAt time.Time
	Kind      string
	PostUri   string
	Uri       string
}
type PostsStruct struct {
	AuthorDid   string
	CreatedAt   time.Time
	IndexedAt   time.Time
	Language    string
	Rank        float64
	ReplyParent string
	ReplyRoot   string
	Uri         string
}
type SubscriptionStateStruct struct {
	Cursor  int64
	Id      int32
	Service string
}
type UsersStruct struct {
	Did            string
	FollowersCount int32
	FollowsCount   int32
	Handle         string
	IndexedAt      time.Time
	LastUpdate     time.Time
	PostsCount     int32
}
