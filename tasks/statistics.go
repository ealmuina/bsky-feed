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

// userDidsBatchSize bounds memory in the StatisticsUpdater loop. The
// WHERE clause in GetUserDidsToRefreshStatistics filters out DIDs whose
// last_update is recent, so each tick naturally processes a new batch
// without needing a cursor or state.
const userDidsBatchSize = 1000

// pdsProfileBatchSize is the number of actors requested per app.bsky.actor.getProfiles
// call. Bluesky allows up to 25 actors in a single request.
const pdsProfileBatchSize = 25

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
	if err := updater.connectXRPCClient(); err != nil {
		return nil, err
	}

	return &updater, nil
}

func (u *StatisticsUpdater) Run() {
	for {
		// Update user statistics (one batch per tick; the WHERE clause
		// itself filters out DIDs updated on previous ticks, so forward
		// progress is automatic).
		dids := u.storageManager.GetOutdatedUserDids(userDidsBatchSize)
		u.updateUserStatisticsBatch(dids)
		time.Sleep(1 * time.Minute)
	}
}

func (u *StatisticsUpdater) connectXRPCClient() error {
	usernameString := os.Getenv("STATISTICS_USER")
	username, err := syntax.ParseAtIdentifier(usernameString)
	if err != nil {
		return err
	}

	client, err := getXRPCClient(
		username,
		os.Getenv("STATISTICS_PASSWORD"),
	)
	if err != nil {
		return err
	}

	u.client = client
	return nil
}

func (u *StatisticsUpdater) deleteUser(did string) {
	delete(u.userLastUpdated, did)
	u.storageManager.DeleteUser(did)
}

func (u *StatisticsUpdater) updateUserStatisticsBatch(dids []string) {
	for i := 0; i < len(dids); i += pdsProfileBatchSize {
		end := i + pdsProfileBatchSize
		if end > len(dids) {
			end = len(dids)
		}
		batch := dids[i:end]

		profiles, err := appbsky.ActorGetProfiles(context.Background(), u.client, batch)
		if err != nil {
			// Batch call failed (e.g. one bad actor or auth issue). Fall back
			// to individual fetches so we can handle per-DID errors.
			for _, did := range batch {
				u.updateUserStatistics(did)
			}
			continue
		}

		// Profiles returned in the batch are refreshed. Any DID from the
		// request that is missing is treated as deactivated/deleted.
		seen := make(map[string]struct{}, len(profiles.Profiles))
		for _, profile := range profiles.Profiles {
			if profile == nil {
				continue
			}
			seen[profile.Did] = struct{}{}
			u.applyProfile(profile)
		}
		for _, did := range batch {
			if _, ok := seen[did]; !ok {
				u.deleteUser(did)
			}
		}
	}
}

func (u *StatisticsUpdater) updateUserStatistics(did string) {
	profile, err := appbsky.ActorGetProfile(context.Background(), u.client, did)
	if err != nil {
		u.handleProfileError(did, err)
		return
	}

	u.applyProfile(profile)
}

func (u *StatisticsUpdater) applyProfile(profile *appbsky.ActorDefs_ProfileViewDetailed) {
	var createdAt time.Time
	if profile.CreatedAt != nil && *profile.CreatedAt != "" {
		if t, parseErr := dateparse.ParseAny(*profile.CreatedAt); parseErr != nil {
			log.Warnf("Error parsing created at for %s: %s", profile.Did, parseErr)
		} else {
			createdAt = t
		}
	}

	userId, err := u.storageManager.GetOrCreateUser(profile.Did)
	if err != nil {
		log.Errorf("Error creating user: %v", err)
		return
	}

	u.storageManager.UpdateUser(
		models.User{
			ID:             userId,
			Did:            profile.Did,
			Handle:         profile.Handle,
			CreatedAt:      createdAt,
			FollowersCount: derefInt64(profile.FollowersCount),
			FollowsCount:   derefInt64(profile.FollowsCount),
		},
	)
}

func (u *StatisticsUpdater) handleProfileError(did string, err error) {
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
					if err := u.connectXRPCClient(); err != nil {
						log.Errorf("Error reconnecting XRPC client: %v", err)
					}
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
}

func derefInt64(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}
