package storage

import (
	"bsky/storage/algorithms"
	"bsky/storage/cache"
	db "bsky/storage/db/sqlc"
	"bsky/storage/models"
	"bsky/utils"
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type Manager struct {
	redisConnection *redis.Client
	dbConnection    *pgxpool.Pool
	queries         *db.Queries
	persistFollows  bool

	usersCache cache.UsersCache
	postsCache cache.PostsCache
	blacklist  Blacklist
	timelines  map[string]cache.Timeline
	algorithms map[string]algorithms.Algorithm

	// Worker pool for limiting concurrent goroutines
	workerPool chan struct{}
	wg         sync.WaitGroup

	// Cleaner tunables, populated in NewManager from env vars. Sizes are
	// int32 to match the sqlc-generated :execrows batch-limit parameter.
	interactionsDeleteBatch int32
	postsDeleteBatch        int32
}

// Cleaner batch sizes. The interactions batch is large because
// ingest is ~22M rows/day; the posts batch is smaller because every
// deleted post fires the user_post_counter trigger UPDATE and cascades
// to its interactions (transitively firing user_interaction_counter).
// cleanerVacuumEveryN controls how often the cleaner runs a non-FULL
// VACUUM (ANALYZE) to return dead tuples to the FSM so future DELETEs
// can reuse pages. A non-FULL vacuum is online — it does not shrink
// the file, but it stops the table from bloating indefinitely.
const (
	defaultInteractionsDeleteBatch = 200000
	defaultPostsDeleteBatch        = 20000
	cleanerVacuumEveryN            = 6

	// Per-pass batch caps. ~22M interactions and ~3M posts cross the 7-day
	// retention boundary every day, so a "drain until short batch" loop can
	// never terminate when delete throughput is below the boundary-crossing
	// rate — that wedge stalled every cleaner tick (timeline trim included)
	// for the entire process lifetime in Aug 2026. Bounding per-pass work
	// guarantees each tick returns; caps comfortably exceed the per-pass
	// crossing rate, so backlogs still drain progressively across passes.
	interactionsMaxBatchesPerPass  = 200 // up to 40M rows per pass
	postsMaxBatchesPerPass         = 250 // up to 5M rows per pass
	oldPostsCacheMaxBatchesPerPass = 100 // up to 1M Redis ids per pass
)

func NewManager(dbConnection *pgxpool.Pool, redisConnection *redis.Client, persistFollows bool) *Manager {
	usersCacheExpiration := utils.IntFromString(
		os.Getenv("USERS_CACHE_EXPIRATION_MINUTES"), 43200,
	)
	postsCacheExpiration := utils.IntFromString(
		os.Getenv("POSTS_CACHE_EXPIRATION_MINUTES"), 1080,
	)
	// Get worker pool size from environment or use default
	workerPoolSize := utils.IntFromString(os.Getenv("STORAGE_WORKER_POOL_SIZE"), 100)

	storageManager := Manager{
		redisConnection: redisConnection,
		dbConnection:    dbConnection,
		queries:         db.New(dbConnection),
		persistFollows:  persistFollows,

		usersCache: cache.NewUsersCache(
			redisConnection,
			time.Duration(usersCacheExpiration)*time.Minute,
		),
		postsCache: cache.NewPostsCache(
			redisConnection,
			time.Duration(postsCacheExpiration)*time.Minute,
		),
		timelines:  make(map[string]cache.Timeline),
		algorithms: make(map[string]algorithms.Algorithm),
		workerPool: make(chan struct{}, workerPoolSize),

		// Cleaner tunables. The defaults assume ~22M interactions/day of
		// ingest (interactions batch 200k = 4.8B rows/day of delete
		// capacity, a 200x headroom). The posts batch is smaller
		// because every delete fires the user_post_counter trigger.
		interactionsDeleteBatch: int32(utils.IntFromString(
			os.Getenv("CLEANER_INTERACTIONS_BATCH_SIZE"), defaultInteractionsDeleteBatch)),
		postsDeleteBatch: int32(utils.IntFromString(
			os.Getenv("CLEANER_POSTS_BATCH_SIZE"), defaultPostsDeleteBatch)),
	}
	storageManager.initializeTimelines()
	storageManager.initializeAlgorithms()
	storageManager.initializeBlacklist()

	return &storageManager
}

func (m *Manager) AddPostToTimeline(timelineName string, timelineEntry models.TimelineEntry) {
	timeline, ok := m.timelines[timelineName]
	if ok {
		timeline.AddPost(timelineEntry)
	} else {
		log.Errorf("Could not find timeline for feed name: %s", timelineName)
	}
}

// CleanOldData orchestrates the periodic cleanup. When the DB is not
// being used for backfill (the production setting) it runs a one-shot
// backfill pass at startup to chew down rows the previous cleaner let
// accumulate, then delegates to CleanTimelinesAndCaches for the cheap
// Redis-only pass. The CLEANER_FORCE_BACKFILL env var lets operators
// skip the startup backfill (e.g. once steady state is reached).
func (m *Manager) CleanOldData(persistentDb bool) {
	ctx := context.Background()
	if !persistentDb {
		if os.Getenv("CLEANER_FORCE_BACKFILL") != "false" {
			log.Info("Running one-shot backfill cleaner pass")
			m.cleanOldInteractions(ctx)
			m.cleanOldPosts(ctx)
		}
	}
	// Timeline/post-cache cleaning is owned by the short-interval cleaner
	// goroutine (tasks.CleanOldData); keeping it out of the hourly DB pass
	// means a stalled Redis-side pass can no longer block the DB prune.
}

// cleanOldInteractions deletes interactions older than 7 days in
// fixed-size batches. The loop exits when the last batch returned
// fewer rows than the batch size — the standard "all matching rows
// consumed" sentinel for this sqlc :execrows pattern. Every
// cleanerVacuumEveryN cycles we issue a non-FULL VACUUM (ANALYZE) so
// dead tuples become available for reuse (and the planner gets fresh
// stats). Non-FULL is online and does not rewrite the file.
func (m *Manager) cleanOldInteractions(ctx context.Context) {
	batchSize := m.interactionsDeleteBatch
	for cycle := 0; cycle < interactionsMaxBatchesPerPass; cycle++ {
		n, err := m.queries.DeleteOldInteractionsBatch(ctx, batchSize)
		if err != nil {
			log.Errorf("Error cleaning old interactions: %v", err)
			return
		}
		if n < int64(batchSize) {
			return
		}
		if cycle%cleanerVacuumEveryN == cleanerVacuumEveryN-1 {
			if _, err := m.dbConnection.Exec(ctx, "VACUUM (ANALYZE) interactions"); err != nil {
				log.Warnf("Non-fatal: vacuum interactions failed: %v", err)
			}
		}
	}
	log.Warnf("cleanOldInteractions hit per-pass cap (%d batches); backlog continues next pass", interactionsMaxBatchesPerPass)
}

// cleanOldPosts deletes posts older than 7 days in fixed-size batches.
// Deleting a post cascades to its interactions (FK ON DELETE CASCADE),
// so this loop also reduces the interactions row count as a side
// effect. The user_interaction_counter trigger is a no-op on cascade
// (the parent post is already gone, so the SELECT inside the trigger
// returns no row). The user_post_counter trigger fires once per
// post, which is the main reason the posts batch is smaller than the
// interactions batch.
func (m *Manager) cleanOldPosts(ctx context.Context) {
	batchSize := m.postsDeleteBatch
	for cycle := 0; cycle < postsMaxBatchesPerPass; cycle++ {
		n, err := m.queries.DeleteOldPostsBatch(ctx, batchSize)
		if err != nil {
			log.Errorf("Error cleaning old posts: %v", err)
			return
		}
		if n < int64(batchSize) {
			return
		}
		if cycle%cleanerVacuumEveryN == cleanerVacuumEveryN-1 {
			if _, err := m.dbConnection.Exec(ctx, "VACUUM (ANALYZE) posts"); err != nil {
				log.Warnf("Non-fatal: vacuum posts failed: %v", err)
			}
		}
	}
	log.Warnf("cleanOldPosts hit per-pass cap (%d batches); backlog continues next pass", postsMaxBatchesPerPass)
}

// CleanTimelinesAndCaches performs the cheap (Redis-only) parts of the
// cleaner: timeline expiry and post cache eviction. Safe to run on a
// short interval because it does not touch Postgres.
func (m *Manager) CleanTimelinesAndCaches() {
	ctx := context.Background()

	// Clean timelines
	for _, timeline := range m.timelines {
		timeline.DeleteExpiredPosts(time.Now().Add(-3 * 24 * time.Hour)) // Timelines lifespan of 3 days
	}

	// Clean caches (process in batches so memory stays bounded). The cutoff is
	// frozen before the loop and the batch count is capped: ~3M posts/day
	// continuously cross the 7-day boundary, so with a live `now()-7d`
	// predicate the loop never saw a short batch and this function never
	// returned — stalling every cleaner tick, timeline trim included, for the
	// whole process lifetime (Aug 2026 outage diagnosis).
	const oldPostsBatchSize = 10000
	cutoff := time.Now().Add(-7 * 24 * time.Hour)
	for batch := 0; batch < oldPostsCacheMaxBatchesPerPass; batch++ {
		oldPosts, err := m.queries.GetOldPosts(ctx, db.GetOldPostsParams{
			Cutoff:    pgtype.Timestamp{Time: cutoff, Valid: true},
			BatchSize: oldPostsBatchSize,
		})
		if err != nil {
			log.Errorf("Error retrieving old posts: %v", err)
			break
		}
		if len(oldPosts) == 0 {
			break
		}
		postIds := make([]int64, 0, len(oldPosts))
		for _, post := range oldPosts {
			postIds = append(postIds, post.ID)
		}
		// Delete from posts cache
		m.postsCache.DeletePosts(postIds)
		if len(oldPosts) < oldPostsBatchSize {
			break
		}
	}
}

func (m *Manager) CreateFollow(follow models.Follow) {
	ctx := context.Background()

	if m.persistFollows {
		result, err := m.queries.CreateFollow(ctx, db.CreateFollowParams{
			UriKey:    follow.UriKey,
			AuthorID:  follow.AuthorID,
			SubjectID: follow.SubjectID,
			CreatedAt: pgtype.Timestamp{Time: follow.CreatedAt, Valid: true},
		})
		if err != nil {
			log.Errorf("Error creating follow: %v", err)
			return
		}
		// Write to DB is done by trigger. Just update caches
		if result.IsCreated {
			m.refreshFollowStatistics(follow.AuthorID, follow.SubjectID, 1, false)
		}
	} else {
		// Do not persist. Just update statistics
		m.refreshFollowStatistics(follow.AuthorID, follow.SubjectID, 1, true)
	}
}

func (m *Manager) CreateInteraction(interaction models.Interaction) {
	ctx := context.Background()

	// Create interaction
	result, err := m.queries.CreateInteraction(ctx, db.CreateInteractionParams{
		UriKey:    interaction.UriKey,
		Kind:      int16(interaction.Kind),
		AuthorID:  interaction.AuthorId,
		PostID:    interaction.PostId,
		CreatedAt: pgtype.Timestamp{Time: interaction.CreatedAt, Valid: true},
	})

	dbOk := err == nil && result.IsCreated
	if err != nil {
		log.Errorf("Error creating interaction: %v", err)
	}

	// Update caches. On FK failure the post isn't in our DB but we still want
	// to credit the engagement so the author's engagement factor is accurate.
	if dbOk {
		m.postsCache.AddInteraction(interaction.PostId)
	}
	if postAuthorId, err := m.GetPostAuthorId(interaction.PostId); err == nil {
		m.usersCache.UpdateUserStatistics(postAuthorId, 0, 0, 0, 1)
	}
}

func (m *Manager) CreatePost(post models.Post) {
	ctx := context.Background()

	// Add post to corresponding timelines
	authorStatistics := m.usersCache.GetUserStatistics(post.AuthorId)
	authorStatistics = m.primeUserStatisticsFromDb(ctx, post.AuthorId, authorStatistics)

	// Acquire a worker from the pool
	select {
	case m.workerPool <- struct{}{}:
		m.wg.Add(1)
		go func() {
			defer func() {
				// Release the worker back to the pool
				<-m.workerPool
				m.wg.Done()
			}()

			if slices.Contains(m.blacklist.Global, post.AuthorDid) {
				// Skip banned accounts
				return
			}

			for timelineName, algorithm := range m.algorithms {
				if ok, reason := algorithm.AcceptsPost(post, authorStatistics); ok {
					m.AddPostToTimeline(
						timelineName,
						models.TimelineEntry{
							Uri:    post.Uri(),
							Reason: reason,
							Rank:   post.Rank,
						},
					)
				}
			}
		}()
	default:
		// If the worker pool is full, process synchronously
		if !slices.Contains(m.blacklist.Global, post.AuthorDid) {
			for timelineName, algorithm := range m.algorithms {
				if ok, reason := algorithm.AcceptsPost(post, authorStatistics); ok {
					m.AddPostToTimeline(
						timelineName,
						models.TimelineEntry{
							Uri:    post.Uri(),
							Reason: reason,
							Rank:   post.Rank,
						},
					)
				}
			}
		}
	}

	upsertResult, err := m.queries.UpsertPost(ctx, db.UpsertPostParams{
		UriKey:        post.UriKey,
		AuthorID:      post.AuthorId,
		ReplyParentID: pgtype.Int8{Int64: post.ReplyParentId, Valid: post.ReplyParentId != 0},
		ReplyRootID:   pgtype.Int8{Int64: post.ReplyRootId, Valid: post.ReplyRootId != 0},
		CreatedAt:     pgtype.Timestamp{Time: post.CreatedAt, Valid: true},
		Language:      pgtype.Text{String: post.Language, Valid: post.Language != ""},
	})
	if err != nil {
		log.Errorf("Error upserting post: %v", err)
		return
	}
	post.ID = upsertResult.ID

	// Update caches. DB update is done by trigger
	if upsertResult.IsCreated {
		m.postsCache.AddPost(post)

		// Exclude replies in users cache
		if post.ReplyRootId == 0 {
			m.usersCache.UpdateUserStatistics(
				post.AuthorId, 0, 0, 1, 0,
			)
		}
	}
}

func (m *Manager) DeleteFollow(identifier models.Identifier) {
	ctx := context.Background()

	if m.persistFollows {
		// Delete follow
		follow, err := m.queries.DeleteFollow(ctx, db.DeleteFollowParams{
			UriKey:   identifier.UriKey,
			AuthorID: identifier.AuthorId,
		})
		if err != nil {
			if !strings.Contains(err.Error(), "no rows in result set") {
				log.Errorf("Error deleting follow: %v", err)
			}
			return
		}

		// Write to DB is done by trigger. Just update caches
		m.refreshFollowStatistics(follow.AuthorID, follow.SubjectID, -1, false)
	} else {
		// Discount from follower statistics directly
		err := m.queries.AddUserFollows(ctx, db.AddUserFollowsParams{
			ID:           identifier.AuthorId,
			FollowsCount: pgtype.Int4{Int32: -1, Valid: true},
		})
		if err == nil {
			m.usersCache.UpdateUserStatistics(
				identifier.AuthorId, -1, 0, 0, 0,
			)
		}
	}
}

func (m *Manager) DeleteInteraction(identifier models.Identifier) {
	ctx := context.Background()

	interaction, err := m.queries.DeleteInteraction(ctx, db.DeleteInteractionParams{
		UriKey:   identifier.UriKey,
		AuthorID: identifier.AuthorId,
	})
	if err != nil {
		if !strings.Contains(err.Error(), "no rows in result set") {
			log.Errorf("Error deleting interaction: %v", err)
		}
		return
	}

	// Update caches
	if m.postsCache.DeleteInteraction(interaction.PostID) {
		if postAuthorId, err := m.GetPostAuthorId(interaction.PostID); err == nil { // post author found
			m.usersCache.UpdateUserStatistics(
				postAuthorId, 0, 0, 0, -1,
			)
		}
	}
}

func (m *Manager) DeletePost(identifier models.Identifier) {
	ctx := context.Background()

	post, err := m.queries.DeletePost(ctx, db.DeletePostParams{
		UriKey:   identifier.UriKey,
		AuthorID: identifier.AuthorId,
	})
	if err != nil {
		if !strings.Contains(err.Error(), "no rows in result set") {
			log.Errorf("Error deleting post: %v", err)
		}
		return
	}

	// Update caches
	m.postsCache.DeletePost(post.ID)

	if !post.ReplyRootID.Valid { // replies are not counted
		postInteractionsCount := m.postsCache.GetPostInteractions(post.ID)
		m.usersCache.UpdateUserStatistics(
			post.AuthorID, 0, 0, -1, -postInteractionsCount,
		)
	}
}

func (m *Manager) DeleteUser(did string) {
	ctx := context.Background()

	// Delete user from DB
	if err := m.queries.DeleteUserByDid(ctx, did); err != nil {
		log.Errorf("Error deleting user %s: %v", did, err)
	}

	// Delete user from cache
	id, ok := m.usersCache.UserDidToId(did)
	if ok {
		m.usersCache.DeleteUser(id)
	}
}

func (m *Manager) HealthCheck(ctx context.Context) error {
	if err := m.redisConnection.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	if err := m.dbConnection.Ping(ctx); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	return nil
}

func (m *Manager) GetCursor(service string) string {
	state, _ := m.queries.GetSubscriptionState(
		context.Background(),
		service,
	)
	return state.Cursor // defaults to "" if not in DB
}

func (m *Manager) GetOrCreateUser(did string) (id int32, err error) {
	// A cached id of 0 is poison from a past DB failure (see below) — treat it as a miss so the
	// mapping self-heals on the next event instead of shadowing the real user for the TTL lifetime.
	if id, ok := m.usersCache.UserDidToId(did); ok && id != 0 {
		return id, nil
	}

	// Best approach based on https://hakibenita.com/postgresql-get-or-create#coming-full-circle
	m.executeTransaction(
		func(ctx context.Context, qtx *db.Queries) {
			// Insert
			err = qtx.CreateUser(ctx, db.CreateUserParams{Did: did})
			if err != nil {
				log.Infof("Error creating user '%s': %v", did, err)
				return
			}
			// Select
			id, err = qtx.GetUserId(ctx, did)
			if err != nil {
				log.Errorf("Error retrieving user id from did '%s': %v", did, err)
				return
			}
		},
	)
	if err != nil || id == 0 {
		// Never cache or hand out a zero id: mapping a did to 0 poisons
		// users_id_to_did/users_did_to_id for the cache TTL, and every caller then
		// attaches posts/interactions to author 0 (FK violations, garbage stats).
		return 0, err
	}
	m.usersCache.AddUser(id, did)

	return id, nil
}

func (m *Manager) GetOutdatedUserDids(limit int32) []string {
	dids, err := m.queries.GetUserDidsToRefreshStatistics(context.Background(), limit)
	if err != nil {
		log.Errorf("Error getting user dids for update: %v", err)
	}
	return dids
}

func (m *Manager) GetPdsSubscriptions() []string {
	result, err := m.queries.GetPdsSubscriptions(context.Background())
	if err != nil {
		log.Errorf("Error getting open pds subscriptions: %v", err)
		return nil
	}
	return result
}

func (m *Manager) GetPostAuthorId(postId int64) (int32, error) {
	if authorId, ok := m.postsCache.GetPostAuthorId(postId); ok {
		return authorId, nil
	}
	return m.queries.GetPostAuthorId(context.Background(), postId)
}

func (m *Manager) GetPostId(authorId int32, uriKey string) (int64, error) {
	ctx := context.Background()

	if postId, ok := m.postsCache.GetPostId(authorId, uriKey); ok {
		return postId, nil
	}
	upsertResult, err := m.queries.UpsertPost(ctx, db.UpsertPostParams{
		AuthorID: authorId,
		UriKey:   uriKey,
	})
	if err != nil {
		return 0, err
	}
	m.postsCache.AddPost(models.Post{
		ID:       upsertResult.ID,
		AuthorId: authorId,
		UriKey:   uriKey,
	})

	return upsertResult.ID, nil
}

func (m *Manager) GetTimeline(timelineName string, maxRank float64, limit int64) []models.TimelineEntry {
	timeline, ok := m.timelines[timelineName]
	if !ok {
		panic(fmt.Sprintf("Could not find timeline for feed: %s", timelineName))
	}
	return timeline.GetPosts(maxRank, limit)
}

func (m *Manager) SetUserMetadata(did string, handle string, createdAt time.Time) {
	ctx := context.Background()
	err := m.queries.SetUserMetadata(ctx, db.SetUserMetadataParams{
		Did:       did,
		Handle:    pgtype.Text{String: handle, Valid: true},
		CreatedAt: pgtype.Timestamp{Time: createdAt, Valid: true},
	})
	if err != nil {
		log.Errorf("Error setting user metadata: %v", err)
	}
	if id, ok := m.usersCache.UserDidToId(did); ok {
		m.usersCache.SetUserCreatedAt(id, createdAt)
	}
}

func (m *Manager) UpdateCursor(service string, cursor string) {
	err := m.queries.UpdateSubscriptionStateCursor(
		context.Background(),
		db.UpdateSubscriptionStateCursorParams{
			Cursor:  cursor,
			Service: service,
		},
	)
	if err != nil {
		log.Errorf("Error updating cursor: %v", err)
	}
}

const userStatisticsReprimeWindow = 5 * time.Minute

func (m *Manager) primeUserStatisticsFromDb(
	ctx context.Context,
	userId int32,
	stats cache.UserStatistics,
) cache.UserStatistics {
	if userId == 0 {
		// 0 is the failure-path sentinel from GetOrCreateUser — no such row exists.
		return stats
	}
	// posts_count and interactions_count are firehose-delta counters with no
	// periodic refresh source. When Redis evicts their hashes (allkeys-lru) or
	// they're lost on restart, they read back as 0 — which makes the engagement
	// factor -1 (posts == 0) or 0 (interactions == 0) and wrongly rejects every
	// top-feed post from that author. Re-prime them from the DB, which holds
	// trigger-maintained authoritative values.
	//
	// Followers/follows are refreshed by StatisticsUpdater on a schedule, but
	// their cache fields carry a TTL: if users stats expire (or Redis evicts
	// them) while the engagement counters stay warm (they're TTL-less firehose
	// delta counters), gating the re-prime only on the engagement counters
	// leaves FollowersCount reading 0 and starves the top feeds — this exact
	// failure starved them in Aug 2026 after a 5-minute users-cache TTL was
	// left configured. So a missing follower count is also a re-prime trigger.
	// The downside (small accounts legitimately read 0 and cause a throttled DB
	// hit per window) is bounded by the throttle below.
	//
	// Re-prime whenever either engagement counter or the followers count is
	// missing (reads 0).
	if stats.PostsCount != 0 && stats.InteractionsCount != 0 && stats.FollowersCount != 0 {
		return stats
	}

	// Only re-prime users we already know about (id<->did mapping exists).
	// A brand-new user with zero counters has not been refreshed yet and
	// should not trigger a DB round-trip on every post.
	if _, ok := m.usersCache.UserIdToDid(userId); !ok {
		return stats
	}

	// Throttle re-prime attempts so a user with genuinely-zero counters
	// (no interactions in the 7-day window) only costs one DB hit per window.
	if m.usersCache.IsUserStatisticsReprimedRecently(userId, userStatisticsReprimeWindow) {
		return stats
	}

	dbUser, err := m.queries.GetUser(ctx, userId)
	if err != nil {
		log.Warnf("Could not re-prime user statistics from DB for user %d: %v", userId, err)
		return stats
	}

	if dbUser.FollowersCount.Valid {
		stats.FollowersCount = int64(dbUser.FollowersCount.Int32)
	}
	if dbUser.FollowsCount.Valid {
		stats.FollowsCount = int64(dbUser.FollowsCount.Int32)
	}
	if stats.FollowersCount != 0 || stats.FollowsCount != 0 {
		m.usersCache.SetUserFollows(userId, stats.FollowersCount, stats.FollowsCount)
	}
	if dbUser.PostsCount.Valid {
		stats.PostsCount = int64(dbUser.PostsCount.Int32)
		m.usersCache.SetUserPostsCount(userId, stats.PostsCount)
	}
	if dbUser.InteractionsCount.Valid {
		stats.InteractionsCount = int64(dbUser.InteractionsCount.Int32)
		m.usersCache.SetUserInteractionsCount(userId, stats.InteractionsCount)
	}
	if dbUser.CreatedAt.Valid {
		stats.CreatedAt = dbUser.CreatedAt.Time
		m.usersCache.SetUserCreatedAt(userId, stats.CreatedAt)
	}

	m.usersCache.MarkUserStatisticsReprimed(userId)
	return stats
}

func (m *Manager) UpdateUser(updatedUser models.User) {
	// Update on cache
	m.usersCache.SetUserFollows(updatedUser.ID, updatedUser.FollowersCount, updatedUser.FollowsCount)
	m.usersCache.SetUserCreatedAt(updatedUser.ID, updatedUser.CreatedAt)

	// Update on DB. posts_count and interactions_count are maintained by DB
	// triggers and are the source of truth for the engagement factor.
	row, err := m.queries.UpdateUser(
		context.Background(),
		db.UpdateUserParams{
			Did:            updatedUser.Did,
			Handle:         pgtype.Text{String: updatedUser.Handle, Valid: true},
			CreatedAt:      pgtype.Timestamp{Time: updatedUser.CreatedAt, Valid: true},
			FollowersCount: pgtype.Int4{Int32: int32(updatedUser.FollowersCount), Valid: true},
			FollowsCount:   pgtype.Int4{Int32: int32(updatedUser.FollowsCount), Valid: true},
			LastUpdate:     pgtype.Timestamp{Time: time.Now(), Valid: true},
		},
	)
	if err != nil {
		log.Errorf("Error updating user: %v", err)
		return
	}

	// Re-prime firehose-derived delta counters from the DB so they survive
	// Redis LRU eviction or restarts.
	if row.PostsCount.Valid {
		m.usersCache.SetUserPostsCount(row.ID, int64(row.PostsCount.Int32))
	}
	if row.InteractionsCount.Valid {
		m.usersCache.SetUserInteractionsCount(row.ID, int64(row.InteractionsCount.Int32))
	}
}

func (m *Manager) executeTransaction(
	operation func(ctx context.Context, queries *db.Queries),
) {
	ctx := context.Background()

	// Start transaction
	tx, err := m.dbConnection.Begin(ctx)
	if err != nil {
		log.Warningf("Error creating transaction: %v", err)
		return
	}
	defer tx.Rollback(ctx) // Rollback on error
	qtx := m.queries.WithTx(tx)

	operation(ctx, qtx)

	// Finish transaction
	err = tx.Commit(ctx)
	if err != nil {
		log.Warningf("Error committing transaction: %v", err)
	}
}

func (m *Manager) initializeAlgorithms() {
	for feedName, algorithm := range algorithms.ImplementedAlgorithms {
		m.algorithms[feedName] = algorithm
	}
}

func (m *Manager) initializeBlacklist() {
	// Open the YAML file
	file, err := os.Open("blacklist.yml")
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}
	defer file.Close()

	// Decode the YAML file
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&m.blacklist); err != nil {
		fmt.Printf("Error decoding YAML: %v\n", err)
		return
	}
}

func (m *Manager) initializeTimelines() {
	for feedName := range algorithms.ImplementedAlgorithms {
		m.timelines[feedName] = cache.NewTimeline(feedName, m.redisConnection)
	}
}

func (m *Manager) refreshFollowStatistics(authorId, subjectId, delta int32, writeToDb bool) {
	ctx := context.Background()
	var err error

	// Count on follower
	if writeToDb {
		err = m.queries.AddUserFollows(ctx, db.AddUserFollowsParams{
			ID:           authorId,
			FollowsCount: pgtype.Int4{Int32: delta, Valid: true},
		})
	}
	if err == nil {
		m.usersCache.UpdateUserStatistics(
			authorId, int64(delta), 0, 0, 0,
		)
	}

	// Count on followed
	if writeToDb {
		err = m.queries.AddUserFollowers(ctx, db.AddUserFollowersParams{
			ID:             subjectId,
			FollowersCount: pgtype.Int4{Int32: delta, Valid: true},
		})
	}
	if err == nil {
		m.usersCache.UpdateUserStatistics(
			subjectId, 0, int64(delta), 0, 0,
		)
	}
}
