// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: copyfrom.go

package db

import (
	"context"
)

// iteratorForBulkCreateFollows implements pgx.CopyFromSource.
type iteratorForBulkCreateFollows struct {
	rows                 []BulkCreateFollowsParams
	skippedFirstNextCall bool
}

func (r *iteratorForBulkCreateFollows) Next() bool {
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

func (r iteratorForBulkCreateFollows) Values() ([]interface{}, error) {
	return []interface{}{
		r.rows[0].UriKey,
		r.rows[0].AuthorID,
		r.rows[0].SubjectID,
		r.rows[0].CreatedAt,
	}, nil
}

func (r iteratorForBulkCreateFollows) Err() error {
	return nil
}

func (q *Queries) BulkCreateFollows(ctx context.Context, arg []BulkCreateFollowsParams) (int64, error) {
	return q.db.CopyFrom(ctx, []string{"tmp_follows"}, []string{"uri_key", "author_id", "subject_id", "created_at"}, &iteratorForBulkCreateFollows{rows: arg})
}

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
		r.rows[0].UriKey,
		r.rows[0].AuthorID,
		r.rows[0].Kind,
		r.rows[0].PostUriKey,
		r.rows[0].PostAuthorID,
		r.rows[0].CreatedAt,
	}, nil
}

func (r iteratorForBulkCreateInteractions) Err() error {
	return nil
}

func (q *Queries) BulkCreateInteractions(ctx context.Context, arg []BulkCreateInteractionsParams) (int64, error) {
	return q.db.CopyFrom(ctx, []string{"tmp_interactions"}, []string{"uri_key", "author_id", "kind", "post_uri_key", "post_author_id", "created_at"}, &iteratorForBulkCreateInteractions{rows: arg})
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
		r.rows[0].UriKey,
		r.rows[0].AuthorID,
		r.rows[0].ReplyParent,
		r.rows[0].ReplyRoot,
		r.rows[0].CreatedAt,
		r.rows[0].Language,
	}, nil
}

func (r iteratorForBulkCreatePosts) Err() error {
	return nil
}

func (q *Queries) BulkCreatePosts(ctx context.Context, arg []BulkCreatePostsParams) (int64, error) {
	return q.db.CopyFrom(ctx, []string{"tmp_posts"}, []string{"uri_key", "author_id", "reply_parent", "reply_root", "created_at", "language"}, &iteratorForBulkCreatePosts{rows: arg})
}