package tasks

import (
	db "bsky/db/sqlc"
	"context"
	"fmt"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"time"
)

const UserChannelBufferSize = 100000

type StatisticsUpdater struct {
	Ch chan string

	queries         *db.Queries
	client          *xrpc.Client
	userLastUpdated map[string]time.Time
}

func getXRPCClient(username *syntax.AtIdentifier, password string) (*xrpc.Client, error) {
	ctx := context.Background()

	dir := identity.DefaultDirectory()
	ident, err := dir.Lookup(ctx, *username)
	if err != nil {
		return nil, err
	}
	pdsURL := ident.PDSEndpoint()
	if pdsURL == "" {
		return nil, fmt.Errorf("empty PDS URL")
	}
	client := xrpc.Client{
		Host: pdsURL,
	}
	sess, err := comatproto.ServerCreateSession(ctx, &client, &comatproto.ServerCreateSession_Input{
		Identifier: ident.DID.String(),
		Password:   password,
	})
	if err != nil {
		return nil, err
	}

	return &xrpc.Client{
		Client: http.DefaultClient,
		Auth: &xrpc.AuthInfo{
			AccessJwt:  sess.AccessJwt,
			RefreshJwt: sess.RefreshJwt,
			Handle:     ident.Handle.String(),
			Did:        ident.DID.String(),
		},
		Host: pdsURL,
	}, nil
}

func NewStatisticsUpdater(queries *db.Queries) (*StatisticsUpdater, error) {
	usernameString := os.Getenv("STATISTICS_USER")
	username, err := syntax.ParseAtIdentifier(usernameString)
	if err != nil {
		return nil, err
	}

	client, err := getXRPCClient(
		username,
		os.Getenv("STATISTICS_PASSWORD"),
	)
	if err != nil {
		return nil, err
	}

	return &StatisticsUpdater{
		Ch:              make(chan string, UserChannelBufferSize),
		queries:         queries,
		client:          client,
		userLastUpdated: make(map[string]time.Time),
	}, nil
}

func (u *StatisticsUpdater) Run() {
	for did := range u.Ch {
		now := time.Now()
		yesterday := now.AddDate(0, 0, -1)

		if u.userLastUpdated[did].Before(yesterday) {
			u.updateUserData(did)
			u.userLastUpdated[did] = now
		}
	}
}

func (u *StatisticsUpdater) updateUserData(did string) {
	ctx := context.Background()

	profile, err := appbsky.ActorGetProfile(ctx, u.client, did)
	if err != nil {
		log.Errorf("Error getting profile: %v", err)
		return
	}

	err = u.queries.UpdateUser(
		ctx,
		db.UpdateUserParams{
			Did:            did,
			Handle:         pgtype.Text{String: profile.Handle, Valid: true},
			FollowersCount: pgtype.Int4{Int32: int32(*profile.FollowersCount), Valid: true},
			FollowsCount:   pgtype.Int4{Int32: int32(*profile.FollowsCount), Valid: true},
			PostsCount:     pgtype.Int4{Int32: int32(*profile.PostsCount), Valid: true},
			LastUpdate:     pgtype.Timestamp{Time: time.Now(), Valid: true},
		},
	)
	if err != nil {
		log.Errorf("Error updating user: %v", err)
		return
	}
}
