package tasks

import (
	db "bsky/db/sqlc"
	"context"
	"errors"
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

const AccountDeactivatedError = "AccountDeactivated"
const InvalidRequestError = "InvalidRequest" // Seen when profile is not found
const ExpiredToken = "ExpiredToken"

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
	updater := StatisticsUpdater{
		queries:         queries,
		userLastUpdated: make(map[string]time.Time),
	}
	updater.connectXRPCClient()

	return &updater, nil
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

func (u *StatisticsUpdater) calculateEngagement(ctx context.Context, did string) float64 {
	engagementFactor, err := u.queries.CalculateUserEngagement(ctx, did)
	if err != nil {
		log.Infof("Error while calculating engagement factor for user %s: %v", did, err)
		return 0
	}
	return engagementFactor
}

func (u *StatisticsUpdater) connectXRPCClient() {
	usernameString := os.Getenv("STATISTICS_USER")
	username, err := syntax.ParseAtIdentifier(usernameString)
	if err != nil {
		panic(err)
	}

	client, err := getXRPCClient(
		username,
		os.Getenv("STATISTICS_PASSWORD"),
	)
	if err != nil {
		panic(err)
	}

	u.client = client
}

func (u *StatisticsUpdater) deleteUser(ctx context.Context, did string) {
	// Delete user
	delete(u.userLastUpdated, did)
	if err := u.queries.DeleteUser(ctx, did); err != nil {
		log.Errorf("Error deleting user %s: %v", did, err)
		return
	}
	// Delete user's posts
	if err := u.queries.DeleteUserPosts(ctx, did); err != nil {
		log.Errorf("Error deleting posts for user %s: %v", did, err)
	}
	// Delete user's interactions
	if err := u.queries.DeleteUser(ctx, did); err != nil {
		log.Errorf("Error deleting user %s: %v", did, err)
	}
}

func (u *StatisticsUpdater) updateUserStatistics() {
	ctx := context.Background()

	dids, err := u.queries.GetUserDidsToRefreshStatistics(ctx)
	if err != nil {
		log.Errorf("Error getting user dids for update: %v", err)
		return
	}

	for _, did := range dids {
		profile, err := appbsky.ActorGetProfile(ctx, u.client, did)
		if err != nil {
			log.Errorf("Error getting profile for user %s: %v", did, err)

			var bskyErr *xrpc.Error

			if errors.As(err, &bskyErr) {
				if bskyErr.StatusCode == 400 {
					var wrappedError *xrpc.XRPCError

					if errors.As(bskyErr.Wrapped, &wrappedError) {
						switch wrappedError.ErrStr {
						case AccountDeactivatedError, InvalidRequestError:
							// Delete user if profile does not exist anymore
							u.deleteUser(ctx, did)
						case ExpiredToken:
							u.connectXRPCClient()
						}
					}
				}

				// Sleep if API rate limit has been exceeded
				if bskyErr.Ratelimit.Remaining == 0 {
					time.Sleep(bskyErr.Ratelimit.Reset.Sub(time.Now()))
				}
			}
			continue
		}

		engagementFactor := u.calculateEngagement(ctx, did)

		err = u.queries.UpdateUser(
			ctx,
			db.UpdateUserParams{
				Did:              profile.Did,
				Handle:           pgtype.Text{String: profile.Handle, Valid: true},
				FollowersCount:   pgtype.Int4{Int32: int32(*profile.FollowersCount), Valid: true},
				FollowsCount:     pgtype.Int4{Int32: int32(*profile.FollowsCount), Valid: true},
				PostsCount:       pgtype.Int4{Int32: int32(*profile.PostsCount), Valid: true},
				EngagementFactor: pgtype.Float8{Float64: engagementFactor, Valid: engagementFactor > 0},
				LastUpdate:       pgtype.Timestamp{Time: time.Now(), Valid: true},
			},
		)
		if err != nil {
			log.Errorf("Error updating user: %v", err)
			continue
		}
	}
}
