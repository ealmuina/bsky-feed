package queries

import (
	"bsky/storage/db/models"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
	"time"
)

func CreateInteraction(session *gocqlx.Session, interaction models.InteractionsStruct) error {
	return session.
		Query(models.Interactions.Insert()).
		BindStruct(interaction).
		Exec()
}

func DeleteInteraction(session *gocqlx.Session, uri string) error {
	return session.
		Query(models.Interactions.Delete()).
		BindMap(qb.M{"uri": uri}).
		Exec()
}

func DeleteInteractionsFromUser(session *gocqlx.Session, authorDid string) error {
	return session.
		Query(
			qb.Delete("interactions").
				Where(qb.Eq("author_did")).
				ToCql(),
		).
		Bind(authorDid).
		Exec()
}

func DeleteOldInteractions(session *gocqlx.Session) error {
	query := session.Query(
		qb.Delete("interactions").
			Where(qb.Lt("created_at")).
			ToCql(),
	).Bind(
		time.Now().AddDate(0, 0, -7).UnixMilli(),
	)
	return query.Exec()
}

func GetInteraction(session *gocqlx.Session, uri string) (models.InteractionsStruct, error) {
	var interaction models.InteractionsStruct

	err := session.
		Query(models.Interactions.Get()).
		BindMap(qb.M{"uri": uri}).
		GetRelease(&interaction)

	return interaction, err
}
