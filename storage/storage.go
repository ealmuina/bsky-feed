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
}

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

func (m *Manager) CleanOldData(persistentDb bool) {
	// Clean DB
	ctx := context.Background()
	if !persistentDb {
		const batchSize = 10000
		for {
			n, err := m.queries.DeleteOldInteractionsBatch(ctx)
			if err != nil {
				log.Errorf("Error cleaning old interactions: %v", err)
				break
			}
			if n < batchSize {
				break
			}
		}
	}

	// Clean timelines
	for _, timeline := range m.timelines {
		timeline.DeleteExpiredPosts(time.Now().Add(-3 * 24 * time.Hour)) // Timelines lifespan of 3 days
	}

	// Clean caches (process in batches so memory stays bounded)
	const oldPostsBatchSize = 10000
	for {
		oldPosts, err := m.queries.GetOldPosts(ctx, oldPostsBatchSize)
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
	if id, ok := m.usersCache.UserDidToId(did); ok {
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
	// posts_count and interactions_count are firehose-delta counters with no
	// periodic refresh source. When Redis evicts their hashes (allkeys-lru) or
	// they're lost on restart, they read back as 0 — which makes the engagement
	// factor -1 (posts == 0) or 0 (interactions == 0) and wrongly rejects every
	// top-feed post from that author. Re-prime them from the DB, which holds
	// trigger-maintained authoritative values.
	//
	// Followers/follows are refreshed by StatisticsUpdater on a schedule, so a
	// missing follower count self-heals on the next refresh tick and is NOT a
	// re-prime trigger: most low-follower accounts legitimately read 0, and
	// re-priming every one of them would be wasted DB load on the hot post path.
	//
	// Re-prime whenever either engagement counter is missing (reads 0). The
	// throttle below bounds the cost for accounts that genuinely have 0
	// interactions in the 7-day window.
	if stats.PostsCount != 0 && stats.InteractionsCount != 0 {
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
