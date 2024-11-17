package queries

import (
	"bsky/storage/db/models"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
)

func CreateFollow(session *gocqlx.Session, follow models.FollowsStruct) error {
	return session.
		Query(models.Follows.Insert()).
		BindStruct(follow).
		Exec()
}

func DeleteFollow(session *gocqlx.Session, uri string) error {
	return session.
		Query(models.Follows.Delete()).
		BindMap(qb.M{"uri": uri}).
		Exec()
}

func DeleteFollowsFromUser(session *gocqlx.Session, authorDid string) error {
	return session.
		Query(models.Follows.Delete()).
		BindMap(qb.M{"author_did": authorDid}).
		Exec()
}

func GetFollow(session *gocqlx.Session, uri string) (models.FollowsStruct, error) {
	var follow models.FollowsStruct

	err := session.
		Query(models.Follows.Get()).
		BindMap(qb.M{"uri": uri}).
		GetRelease(&follow)

	return follow, err
}
