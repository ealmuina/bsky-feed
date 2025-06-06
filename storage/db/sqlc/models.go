// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.28.0

package db

import (
	"github.com/jackc/pgx/v5/pgtype"
)

type Follow struct {
	ID        int64
	UriKey    string
	AuthorID  int32
	SubjectID int32
	CreatedAt pgtype.Timestamp
}

type Interaction struct {
	ID        int64
	UriKey    string
	AuthorID  int32
	Kind      int16
	PostID    int64
	CreatedAt pgtype.Timestamp
}

type Post struct {
	ID            int64
	UriKey        string
	AuthorID      int32
	ReplyParentID pgtype.Int8
	ReplyRootID   pgtype.Int8
	Language      pgtype.Text
	CreatedAt     pgtype.Timestamp
}

type SubscriptionState struct {
	ID      int32
	Service string
	Cursor  string
}

type TmpFollow struct {
	ID        int64
	UriKey    string
	AuthorID  int32
	SubjectID int32
	CreatedAt pgtype.Timestamp
}

type TmpInteraction struct {
	ID        int64
	UriKey    string
	AuthorID  int32
	Kind      int16
	PostID    int64
	CreatedAt pgtype.Timestamp
}

type TmpPost struct {
	ID            int64
	UriKey        string
	AuthorID      int32
	ReplyParentID pgtype.Int8
	ReplyRootID   pgtype.Int8
	Language      pgtype.Text
	CreatedAt     pgtype.Timestamp
}

type User struct {
	ID               int32
	Did              string
	Handle           pgtype.Text
	FollowersCount   pgtype.Int4
	FollowsCount     pgtype.Int4
	PostsCount       pgtype.Int4
	CreatedAt        pgtype.Timestamp
	LastUpdate       pgtype.Timestamp
	RefreshFrequency pgtype.Int4
}
