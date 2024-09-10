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
	"math"
	"net/http"
	"os"
	"time"
)

const ActorGetProfilesMaxSize = 25

type StatisticsUpdater struct {
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
		queries:         queries,
		client:          client,
		userLastUpdated: make(map[string]time.Time),
	}, nil
}

func (u *StatisticsUpdater) Run() {
	u.updateUserStatistics()
	for {
		select {
		case <-time.After(1 * time.Hour):
			u.updateUserStatistics()
		}
	}
}

func (u *StatisticsUpdater) updateUserStatistics() {
	ctx := context.Background()

	dids, err := u.queries.GetUserDidsToRefreshStatistics(ctx)
	if err != nil {
		log.Errorf("Error getting user dids for update: %v", err)
		return
	}
	numBatches := int(math.Ceil(
		float64(len(dids)) / ActorGetProfilesMaxSize,
	))

	for i := 0; i < numBatches; i++ {
		startIndex := i * ActorGetProfilesMaxSize
		endIndex := int(math.Min(float64(startIndex+ActorGetProfilesMaxSize), float64(len(dids))))

		profiles, err := appbsky.ActorGetProfiles(ctx, u.client, dids[startIndex:endIndex])
		if err != nil {
			log.Errorf("Error getting user profiles: %v", err)
			continue
		}

		for _, profile := range profiles.Profiles {
			err = u.queries.UpdateUser(
				ctx,
				db.UpdateUserParams{
					Did:            profile.Did,
					Handle:         pgtype.Text{String: profile.Handle, Valid: true},
					FollowersCount: pgtype.Int4{Int32: int32(*profile.FollowersCount), Valid: true},
					FollowsCount:   pgtype.Int4{Int32: int32(*profile.FollowsCount), Valid: true},
					PostsCount:     pgtype.Int4{Int32: int32(*profile.PostsCount), Valid: true},
					LastUpdate:     pgtype.Timestamp{Time: time.Now(), Valid: true},
				},
			)
			if err != nil {
				log.Errorf("Error updating user: %v", err)
				continue
			}
		}
	}
}
