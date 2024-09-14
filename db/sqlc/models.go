// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0

package db

import (
	"database/sql/driver"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

type InteractionType string

const (
	InteractionTypeLike   InteractionType = "like"
	InteractionTypeRepost InteractionType = "repost"
)

func (e *InteractionType) Scan(src interface{}) error {
	switch s := src.(type) {
	case []byte:
		*e = InteractionType(s)
	case string:
		*e = InteractionType(s)
	default:
		return fmt.Errorf("unsupported scan type for InteractionType: %T", src)
	}
	return nil
}

type NullInteractionType struct {
	InteractionType InteractionType
	Valid           bool // Valid is true if InteractionType is not NULL
}

// Scan implements the Scanner interface.
func (ns *NullInteractionType) Scan(value interface{}) error {
	if value == nil {
		ns.InteractionType, ns.Valid = "", false
		return nil
	}
	ns.Valid = true
	return ns.InteractionType.Scan(value)
}

// Value implements the driver Valuer interface.
func (ns NullInteractionType) Value() (driver.Value, error) {
	if !ns.Valid {
		return nil, nil
	}
	return string(ns.InteractionType), nil
}

type Interaction struct {
	Uri       string
	Cid       string
	Kind      InteractionType
	AuthorDid string
	PostUri   string
	IndexedAt pgtype.Timestamp
	CreatedAt pgtype.Timestamp
}

type Post struct {
	Uri         string
	AuthorDid   string
	Cid         string
	ReplyParent pgtype.Text
	ReplyRoot   pgtype.Text
	IndexedAt   pgtype.Timestamp
	CreatedAt   pgtype.Timestamp
	Language    pgtype.Text
}

type SubscriptionState struct {
	ID      int32
	Service string
	Cursor  int64
}

type User struct {
	Did            string
	Handle         pgtype.Text
	FollowersCount pgtype.Int4
	FollowsCount   pgtype.Int4
	PostsCount     pgtype.Int4
	IndexedAt      pgtype.Timestamp
	LastUpdate     pgtype.Timestamp
}
