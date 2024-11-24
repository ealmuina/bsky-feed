// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0

package db

import (
	"github.com/jackc/pgx/v5/pgtype"
)

type Interaction struct {
	ID           int32
	UriKey       string
	AuthorID     int32
	Kind         int16
	PostUriKey   string
	PostAuthorID int32
	CreatedAt    pgtype.Timestamp
}

type Post struct {
	ID          int32
	UriKey      string
	AuthorID    int32
	ReplyParent []string
	ReplyRoot   []string
	Language    pgtype.Text
	CreatedAt   pgtype.Timestamp
}

type SubscriptionState struct {
	ID      int32
	Service string
	Cursor  int64
}

type TmpInteraction struct {
	ID           int32
	UriKey       string
	AuthorID     int32
	Kind         int16
	PostUriKey   string
	PostAuthorID int32
	CreatedAt    pgtype.Timestamp
}

type TmpPost struct {
	ID          int32
	UriKey      string
	AuthorID    int32
	ReplyParent []string
	ReplyRoot   []string
	Language    pgtype.Text
	CreatedAt   pgtype.Timestamp
}

type User struct {
	ID               int32
	Did              string
	Handle           pgtype.Text
	FollowersCount   pgtype.Int4
	FollowsCount     pgtype.Int4
	PostsCount       pgtype.Int4
	Follows          []byte
	LastUpdate       pgtype.Timestamp
	RefreshFrequency int32
}
