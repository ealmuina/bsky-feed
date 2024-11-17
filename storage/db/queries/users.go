package queries

import (
	"bsky/storage/db/models"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
	"time"
)

func ApplyFollowToUsers(session *gocqlx.Session, follow models.FollowsStruct, delta int) error {
	query := session.Query(
		models.UsersCounters.
			UpdateBuilder().
			Add("follows_count").
			ToCql(),
	).Bind(
		delta,
		follow.AuthorDid,
	)
	if err := query.Exec(); err != nil {
		return err
	}

	query = session.Query(
		models.UsersCounters.
			UpdateBuilder().
			Add("followers_count").
			ToCql(),
	).Bind(
		delta,
		follow.SubjectDid,
	)
	return query.Exec()
}

func ApplyPostToUser(session *gocqlx.Session, post models.PostsStruct, delta int) error {
	return session.Query(
		models.UsersCounters.
			UpdateBuilder().
			Add("posts_count").
			ToCql(),
	).Bind(
		delta,
		post.AuthorDid,
	).Exec()
}

func CreateUser(session *gocqlx.Session, user models.UsersStruct) error {
	return session.
		Query(models.Users.Insert()).
		BindStruct(user).
		Exec()
}

func DeleteUser(session *gocqlx.Session, did string) error {
	return session.
		Query(models.Users.Delete()).
		BindMap(qb.M{"did": did}).
		Exec()
}

func GetUserDids(session *gocqlx.Session) ([]string, error) {
	var users []models.UsersStruct
	query := session.Query(models.Users.Select("did"))
	if err := query.SelectRelease(&users); err != nil {
		return nil, err
	}

	dids := make([]string, 0, len(users))
	for _, user := range users {
		dids = append(dids, user.Did)
	}
	return dids, nil
}

func GetUserDidsToRefreshStatistics(session *gocqlx.Session) ([]string, error) {
	query := session.Query(
		qb.Select("users").
			Columns("did").
			Where(qb.Lt("last_update")).
			ToCql(),
	).Bind(
		time.Now().AddDate(0, 0, -30).UnixMilli(),
	)

	var users []models.UsersStruct
	if err := query.SelectRelease(&users); err != nil {
		return nil, err
	}

	dids := make([]string, 0, len(users))
	for _, user := range users {
		dids = append(dids, user.Did)
	}
	return dids, nil
}

func UpdateUser(session *gocqlx.Session, updatedUser models.UsersStruct) error {
	// Delete if exists
	_ = DeleteUser(session, updatedUser.Did)

	return CreateUser(session, updatedUser)
}

func UpdateUserCounters(session *gocqlx.Session, updatedUserCounters models.UsersCountersStruct) error {
	// Delete previous entry from DB
	query := session.Query(
		models.UsersCounters.Delete(),
	).BindStruct(updatedUserCounters)

	if err := query.Exec(); err != nil {
		return err
	}

	// Add new entry
	return session.Query(
		models.UsersCounters.UpdateBuilder().
			Add("follows_count").
			Add("followers_count").
			Add("posts_count").
			ToCql(),
	).Bind(
		updatedUserCounters.FollowsCount,
		updatedUserCounters.FollowersCount,
		updatedUserCounters.PostsCount,
		updatedUserCounters.Did,
	).Exec()
}
