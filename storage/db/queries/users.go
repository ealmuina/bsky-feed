package queries

import (
	"bsky/storage/db/models"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
	"time"
)

func increaseUserCounter(session *gocqlx.Session, userDid string, counterField string, delta int) error {
	if countersInitialized := isUserCountersInitialized(session, userDid); !countersInitialized {
		return nil
	}

	query := session.Query(
		models.UsersCounters.
			UpdateBuilder().
			Add(counterField).
			ToCql(),
	).Bind(
		delta, userDid,
	)
	return query.Exec()
}

func isUserCountersInitialized(session *gocqlx.Session, userDid string) bool {
	query := session.Query(
		models.UsersCounters.Get("did"),
	).Bind(
		userDid,
	)
	var userCounters models.UsersCountersStruct
	if err := query.GetRelease(&userCounters); err != nil {
		return false
	}
	return true
}

func ApplyFollowToUsers(session *gocqlx.Session, follow models.FollowsStruct, delta int) error {
	err := increaseUserCounter(session, follow.AuthorDid, "follows_count", delta)
	if err != nil {
		return err
	}
	return increaseUserCounter(session, follow.SubjectDid, "followers_count", delta)
}

func ApplyPostToUser(session *gocqlx.Session, post models.PostsStruct, delta int) error {
	return increaseUserCounter(session, post.AuthorDid, "posts_count", delta)
}

func CreateUser(session *gocqlx.Session, user models.UsersStruct) error {
	if isUserCountersInitialized(session, user.Did) {
		return nil
	}
	return session.
		Query(models.Users.InsertBuilder().Unique().ToCql()).
		BindStruct(user).
		Exec()
}

func DeleteUser(session *gocqlx.Session, did string) error {
	return session.
		Query(qb.Delete("users").Where(qb.Eq("did")).ToCql()).
		Bind(did).
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
			AllowFiltering().
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
	// Delete outdated entry
	_ = session.
		Query(qb.Delete("users").Where(qb.Eq("did")).ToCql()).
		Bind(updatedUser.Did).
		Exec()

	// Insert updated entry
	return session.
		Query(models.Users.InsertBuilder().ToCql()).
		BindStruct(updatedUser).
		Exec()
}

func UpdateUserCounters(session *gocqlx.Session, updatedUserCounters models.UsersCountersStruct) error {
	if countersInitialized := isUserCountersInitialized(session, updatedUserCounters.Did); countersInitialized {
		return nil
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
