package queries

import (
	"bsky/storage/db/models"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
)

func GetSubscriptionState(session *gocqlx.Session, service string) (models.SubscriptionStateStruct, error) {
	var subscriptionState models.SubscriptionStateStruct

	err := session.
		Query(models.SubscriptionState.Get()).
		BindMap(qb.M{"service": service}).
		GetRelease(&subscriptionState)

	return subscriptionState, err
}

func UpdateSubscriptionStateCursor(session *gocqlx.Session, service string, cursor int64) error {
	return session.Query(
		models.Users.
			UpdateBuilder().
			Set("cursor").
			Where(qb.Eq("service")).
			ToCql(),
	).Bind(
		cursor,
		service,
	).Exec()
}
