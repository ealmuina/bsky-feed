// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: copyfrom.go

package db

import (
	"context"
)

// iteratorForBulkCreateInteractions implements pgx.CopyFromSource.
type iteratorForBulkCreateInteractions struct {
	rows                 []BulkCreateInteractionsParams
	skippedFirstNextCall bool
}

func (r *iteratorForBulkCreateInteractions) Next() bool {
	if len(r.rows) == 0 {
		return false
	}
	if !r.skippedFirstNextCall {
		r.skippedFirstNextCall = true
		return true
	}
	r.rows = r.rows[1:]
	return len(r.rows) > 0
}

func (r iteratorForBulkCreateInteractions) Values() ([]interface{}, error) {
	return []interface{}{
		r.rows[0].Uri,
		r.rows[0].Kind,
		r.rows[0].AuthorDid,
		r.rows[0].PostUri,
		r.rows[0].CreatedAt,
	}, nil
}

func (r iteratorForBulkCreateInteractions) Err() error {
	return nil
}

func (q *Queries) BulkCreateInteractions(ctx context.Context, arg []BulkCreateInteractionsParams) (int64, error) {
	return q.db.CopyFrom(ctx, []string{"interactions"}, []string{"uri", "kind", "author_did", "post_uri", "created_at"}, &iteratorForBulkCreateInteractions{rows: arg})
}

// iteratorForBulkCreatePosts implements pgx.CopyFromSource.
type iteratorForBulkCreatePosts struct {
	rows                 []BulkCreatePostsParams
	skippedFirstNextCall bool
}

func (r *iteratorForBulkCreatePosts) Next() bool {
	if len(r.rows) == 0 {
		return false
	}
	if !r.skippedFirstNextCall {
		r.skippedFirstNextCall = true
		return true
	}
	r.rows = r.rows[1:]
	return len(r.rows) > 0
}

func (r iteratorForBulkCreatePosts) Values() ([]interface{}, error) {
	return []interface{}{
		r.rows[0].Uri,
		r.rows[0].AuthorDid,
		r.rows[0].ReplyParent,
		r.rows[0].ReplyRoot,
		r.rows[0].CreatedAt,
		r.rows[0].Language,
		r.rows[0].Rank,
	}, nil
}

func (r iteratorForBulkCreatePosts) Err() error {
	return nil
}

func (q *Queries) BulkCreatePosts(ctx context.Context, arg []BulkCreatePostsParams) (int64, error) {
	return q.db.CopyFrom(ctx, []string{"posts"}, []string{"uri", "author_did", "reply_parent", "reply_root", "created_at", "language", "rank"}, &iteratorForBulkCreatePosts{rows: arg})
}
