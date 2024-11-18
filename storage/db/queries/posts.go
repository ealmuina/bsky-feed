package queries

import (
	"bsky/storage/db/models"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
	"time"
)

func CreatePost(session *gocqlx.Session, post models.PostsStruct) error {
	return session.
		Query(models.Posts.InsertBuilder().Unique().ToCql()).
		BindStruct(post).
		Exec()
}

func DeleteOldPosts(session *gocqlx.Session) ([]models.PostsStruct, error) {
	// Retrieve posts
	query := session.Query(
		qb.Select("posts").
			Columns("uri", "author_id", "created_at").
			Where(qb.Lt("created_at")).
			ToCql(),
	).Bind(
		time.Now().AddDate(0, 0, -7).UnixMilli(),
	)
	var postsToDelete []models.PostsStruct
	if err := query.SelectRelease(&postsToDelete); err != nil {
		return nil, err
	}
	uris := make([]string, 0, len(postsToDelete))
	for _, post := range postsToDelete {
		uris = append(uris, post.Uri)
	}

	// Delete posts
	query = session.Query(
		qb.Delete("posts").
			Where(qb.In("uri")).
			ToCql(),
	).Bind(
		uris,
	)
	if err := query.Exec(); err != nil {
		return nil, err
	}

	return postsToDelete, nil
}

func DeletePost(session *gocqlx.Session, uri string) error {
	return session.
		Query(models.Posts.Delete()).
		BindMap(qb.M{"uri": uri}).
		Exec()
}

func DeletePostsFromUser(session *gocqlx.Session, authorDid string) error {
	return session.
		Query(
			qb.Delete("posts").
				Where(qb.Eq("author_did")).
				ToCql(),
		).
		Bind(authorDid).
		Exec()
}

func GetLanguagePosts(
	session *gocqlx.Session,
	language string,
	rank float64,
	limit uint,
) ([]models.PostsStruct, error) {
	var posts []models.PostsStruct

	query := session.Query(
		qb.Select("posts").
			Where(
				qb.Eq("language"),
				qb.Eq("reply_root"),
				qb.Lt("rank"),
			).
			Limit(limit).
			ToCql(),
	).Bind(
		language, "", rank,
	)

	if err := query.SelectRelease(&posts); err != nil {
		return nil, err
	}
	return posts, nil
}

func GetPost(session *gocqlx.Session, uri string) (models.PostsStruct, error) {
	var post models.PostsStruct

	err := session.
		Query(models.Posts.Get()).
		BindMap(qb.M{"uri": uri}).
		GetRelease(&post)

	return post, err
}
