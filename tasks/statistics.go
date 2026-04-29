package tasks

import (
	"bsky/storage"
	"bsky/storage/models"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/araddon/dateparse"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
	log "github.com/sirupsen/logrus"
)

const AccountDeactivatedError = "AccountDeactivated"
const InvalidRequestError = "InvalidRequest" // Seen when profile is not found
const ExpiredToken = "ExpiredToken"

type StatisticsUpdater struct {
	client          *xrpc.Client
	storageManager  *storage.Manager
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
		Client: &http.Client{Timeout: 30 * time.Second},
		Auth: &xrpc.AuthInfo{
			AccessJwt:  sess.AccessJwt,
			RefreshJwt: sess.RefreshJwt,
			Handle:     ident.Handle.String(),
			Did:        ident.DID.String(),
		},
		Host: pdsURL,
	}, nil
}

func NewStatisticsUpdater(storageManager *storage.Manager) (*StatisticsUpdater, error) {
	updater := StatisticsUpdater{
		storageManager:  storageManager,
		userLastUpdated: make(map[string]time.Time),
	}
	updater.connectXRPCClient()

	return &updater, nil
}

func (u *StatisticsUpdater) Run() {
	for {
		// Update user statistics
		dids := u.storageManager.GetOutdatedUserDids()
		for _, did := range dids {
			u.updateUserStatistics(did)
		}
		time.Sleep(1 * time.Minute)
	}
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

func (u *StatisticsUpdater) deleteUser(did string) {
	delete(u.userLastUpdated, did)
	u.storageManager.DeleteUser(did)
}

func (u *StatisticsUpdater) updateUserStatistics(did string) {
	profile, err := appbsky.ActorGetProfile(context.Background(), u.client, did)
	if err != nil {
		var bskyErr *xrpc.Error

		if errors.As(err, &bskyErr) {
			if bskyErr.StatusCode == 400 {
				var wrappedError *xrpc.XRPCError

				if errors.As(bskyErr.Wrapped, &wrappedError) {
					switch wrappedError.ErrStr {
					case AccountDeactivatedError, InvalidRequestError:
						// Delete user if profile does not exist anymore
						u.deleteUser(did)
					case ExpiredToken:
						u.connectXRPCClient()
					}
				}
			} else {
				log.Errorf("Error getting profile for user %s: %v", did, err)
			}

			// Sleep if API rate limit has been exceeded
			if bskyErr.Ratelimit != nil && bskyErr.Ratelimit.Remaining == 0 {
				if d := bskyErr.Ratelimit.Reset.Sub(time.Now()); d > 0 {
					time.Sleep(d)
				}
			}
		}
		return
	}

	var createdAt time.Time
	if profile.CreatedAt != nil && *profile.CreatedAt != "" {
		if t, parseErr := dateparse.ParseAny(*profile.CreatedAt); parseErr != nil {
			log.Warnf("Error parsing created at for %s: %s", did, parseErr)
		} else {
			createdAt = t
		}
	}

	userId, err := u.storageManager.GetOrCreateUser(did)
	if err != nil {
		log.Errorf("Error creating user: %v", err)
		return
	}

	u.storageManager.UpdateUser(
		models.User{
			ID:             userId,
			Did:            did,
			Handle:         profile.Handle,
			CreatedAt:      createdAt,
			FollowersCount: *profile.FollowersCount,
			FollowsCount:   *profile.FollowsCount,
			PostsCount:     *profile.PostsCount,
		},
	)
}
