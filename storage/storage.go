package storage

import (
	"bsky/storage/algorithms"
	"bsky/storage/cache"
	db "bsky/storage/db/sqlc"
	"bsky/storage/models"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

const PostsToCreateBulkSize = 100
const InteractionsToCreateBulkSize = 100
const PostsToDeleteBulkSize = 100
const InteractionsToDeleteBulkSize = 100
const DataLifespanDays = 7

type Manager struct {
	redisConnection *redis.Client
	dbConnection    *pgxpool.Pool
	queries         *db.Queries

	usersCache cache.UsersCache
	postsCache cache.PostsCache
	timelines  map[string]cache.Timeline
	algorithms map[string]algorithms.Algorithm

	usersCreated              sync.Map
	postsToCreateMutex        sync.Mutex
	postsToCreate             []db.BulkCreatePostsParams
	interactionsToCreateMutex sync.Mutex
	interactionsToCreate      []db.BulkCreateInteractionsParams
	postsToDeleteMutex        sync.Mutex
	postsToDelete             []string
	interactionsToDeleteMutex sync.Mutex
	interactionsToDelete      []string
}

func NewManager(dbConnection *pgxpool.Pool, redisConnection *redis.Client) *Manager {
	storageManager := Manager{
		redisConnection: redisConnection,
		dbConnection:    dbConnection,
		queries:         db.New(dbConnection),

		usersCache: cache.NewUsersCache(
			redisConnection,
			30*24*time.Hour, // expire entries after 30 days
		),
		postsCache: cache.NewPostsCache(
			redisConnection,
			7*24*time.Hour, // expire entries after 7 days
		),
		timelines:  make(map[string]cache.Timeline),
		algorithms: make(map[string]algorithms.Algorithm),

		usersCreated:              sync.Map{},
		postsToCreateMutex:        sync.Mutex{},
		postsToCreate:             make([]db.BulkCreatePostsParams, 0, PostsToCreateBulkSize),
		interactionsToCreateMutex: sync.Mutex{},
		interactionsToCreate:      make([]db.BulkCreateInteractionsParams, 0, InteractionsToCreateBulkSize),
		postsToDeleteMutex:        sync.Mutex{},
		postsToDelete:             make([]string, 0, PostsToDeleteBulkSize),
		interactionsToDeleteMutex: sync.Mutex{},
		interactionsToDelete:      make([]string, 0, InteractionsToDeleteBulkSize),
	}
	storageManager.loadUsersCreated()
	storageManager.loadUsersFollows()
	storageManager.initializeTimelines()
	storageManager.initializeAlgorithms()
	return &storageManager
}

func (m *Manager) AddPostToTimeline(timelineName string, post models.Post) {
	timeline, ok := m.timelines[timelineName]
	if ok {
		timeline.AddPost(post)
	} else {
		log.Errorf("Could not find timeline for feed name: %s", timelineName)
	}
}

func (m *Manager) CleanOldData() {
	// Clean DB
	ctx := context.Background()
	if err := m.queries.DeleteOldPosts(ctx); err != nil {
		log.Errorf("Error cleaning old posts: %m", err)
	}
	if err := m.queries.DeleteOldInteractions(ctx); err != nil {
		log.Errorf("Error cleaning old interactions: %m", err)
	}

	// Clean timelines
	for _, timeline := range m.timelines {
		timeline.DeleteExpiredPosts(time.Now().Add(-DataLifespanDays * 24 * time.Hour))
	}
}

func (m *Manager) CreateFollow(follow models.Follow) {
	ctx := context.Background()

	// Start transaction
	tx, err := m.dbConnection.Begin(ctx)
	if err != nil {
		log.Warningf("Error creating transaction: %v", err)
	}
	defer tx.Rollback(ctx) // Rollback on error
	qtx := m.queries.WithTx(tx)

	// Create follow
	err = qtx.CreateFollow(ctx, db.CreateFollowParams{
		Uri:        follow.Uri,
		AuthorDid:  follow.AuthorDid,
		SubjectDid: follow.SubjectDid,
		CreatedAt:  pgtype.Timestamp{Time: follow.CreatedAt, Valid: true},
	})
	if err != nil {
		log.Warningf("Error creating follow: %v", err)
		return
	}

	// Add follow to users statistics
	err = qtx.AddUserFollows(ctx, db.AddUserFollowsParams{
		Did:          follow.AuthorDid,
		FollowsCount: pgtype.Int4{Int32: 1, Valid: true},
	})
	if err == nil {
		m.usersCache.UpdateUserStatistics(
			follow.AuthorDid, 1, 0, 0, 0,
		)
	}
	err = qtx.AddUserFollowers(ctx, db.AddUserFollowersParams{
		Did:            follow.SubjectDid,
		FollowersCount: pgtype.Int4{Int32: 1, Valid: true},
	})
	if err == nil {
		m.usersCache.UpdateUserStatistics(
			follow.SubjectDid, 0, 1, 0, 0,
		)
	}

	// Finish transaction
	tx.Commit(ctx)
}

func (m *Manager) CreateInteraction(interaction models.Interaction) error {
	ctx := context.Background()

	m.interactionsToCreateMutex.Lock()
	defer m.interactionsToCreateMutex.Unlock()

	m.interactionsToCreate = append(
		m.interactionsToCreate,
		db.BulkCreateInteractionsParams{
			Uri:       interaction.Uri,
			Kind:      db.InteractionType(interaction.Kind),
			AuthorDid: interaction.AuthorDid,
			PostUri:   interaction.PostUri,
			CreatedAt: pgtype.Timestamp{Time: interaction.CreatedAt, Valid: true},
		},
	)

	if len(m.interactionsToCreate) >= InteractionsToCreateBulkSize {
		// Clone buffer and exec bulk insert
		go func(interactions []db.BulkCreateInteractionsParams) {
			if _, err := m.queries.BulkCreateInteractions(ctx, interactions); err != nil {
				log.Errorf("Error creating interactions: %v", err)
			}

			// Update caches
			for _, interaction := range interactions {
				postAuthor, ok := m.postsCache.GetPostAuthor(interaction.PostUri)
				if ok {
					m.postsCache.AddInteraction(interaction.PostUri)
					m.usersCache.UpdateUserStatistics(
						postAuthor, 0, 0, 0, 1,
					)
				}
			}
		}(m.interactionsToCreate)

		// Clear buffer
		m.interactionsToCreate = make([]db.BulkCreateInteractionsParams, 0, InteractionsToCreateBulkSize)
	}

	return nil
}

func (m *Manager) CreatePost(post models.Post) {
	// Add post to corresponding timelines
	authorStatistics := m.usersCache.GetUserStatistics(post.AuthorDid)
	for timelineName, algorithm := range m.algorithms {
		if ok, reason := algorithm.AcceptsPost(post, authorStatistics); ok {
			post.Reason = reason
			m.AddPostToTimeline(timelineName, post)
		}
	}

	// Store in cache (exclude replies)
	if post.ReplyRoot == "" {
		m.postsCache.AddPost(post)
	}

	// Store in DB
	m.postsToCreateMutex.Lock()
	defer m.postsToCreateMutex.Unlock()

	m.postsToCreate = append(
		m.postsToCreate,
		db.BulkCreatePostsParams{
			Uri:         post.Uri,
			AuthorDid:   post.AuthorDid,
			ReplyParent: pgtype.Text{String: post.ReplyParent, Valid: post.ReplyParent != ""},
			ReplyRoot:   pgtype.Text{String: post.ReplyRoot, Valid: post.ReplyRoot != ""},
			CreatedAt:   pgtype.Timestamp{Time: post.CreatedAt, Valid: true},
			Language:    pgtype.Text{String: post.Language, Valid: post.Language != ""},
			Rank:        pgtype.Float8{Float64: post.Rank, Valid: true},
		},
	)
	if len(m.postsToCreate) >= PostsToCreateBulkSize {
		// Clone buffer and exec bulk insert
		go func(posts []db.BulkCreatePostsParams) {
			ctx := context.Background()

			// Start transaction
			tx, err := m.dbConnection.Begin(ctx)
			if err != nil {
				log.Warningf("Error creating transaction: %v", err)
			}
			defer tx.Rollback(ctx) // Rollback on error
			qtx := m.queries.WithTx(tx)

			// Create posts
			if _, err := qtx.BulkCreatePosts(context.Background(), posts); err != nil {
				log.Errorf("Error creating posts: %v", err)
				return
			}

			// Add post to user statistics
			for _, post := range posts {
				err = qtx.AddUserPosts(ctx, db.AddUserPostsParams{
					Did:        post.AuthorDid,
					PostsCount: pgtype.Int4{Int32: 1, Valid: true},
				})
				if err == nil {
					// Update cache
					m.usersCache.UpdateUserStatistics(
						post.AuthorDid, 0, 0, 1, 0,
					)
				}
			}

			// Finish transaction
			tx.Commit(ctx)
		}(
			m.postsToCreate,
		)

		// Clear buffer
		m.postsToCreate = make([]db.BulkCreatePostsParams, 0, PostsToCreateBulkSize)
	}
}

func (m *Manager) CreateUser(did string) {
	if _, ok := m.usersCreated.Load(did); ok {
		return
	}
	err := m.queries.CreateUser(
		context.Background(),
		db.CreateUserParams{Did: did},
	)
	if err != nil {
		log.Errorf("Error creating user: %m", err)
		return
	}
	m.usersCreated.Store(did, true)
}

func (m *Manager) DeleteFollow(uri string) {
	ctx := context.Background()

	// Start transaction
	tx, err := m.dbConnection.Begin(ctx)
	if err != nil {
		log.Warningf("Error creating transaction: %v", err)
	}
	defer tx.Rollback(ctx) // Rollback on error
	qtx := m.queries.WithTx(tx)

	// Create follow
	follow, err := qtx.DeleteFollow(ctx, uri)
	if err != nil {
		log.Infof("Error deleting follow: %v", err)
		return
	}

	// Remove follow from users statistics
	err = qtx.AddUserFollows(ctx, db.AddUserFollowsParams{
		Did:          follow.AuthorDid,
		FollowsCount: pgtype.Int4{Int32: -1, Valid: true},
	})
	if err == nil {
		m.usersCache.UpdateUserStatistics(
			follow.AuthorDid, -1, 0, 0, 0,
		)
	}
	err = qtx.AddUserFollowers(ctx, db.AddUserFollowersParams{
		Did:            follow.SubjectDid,
		FollowersCount: pgtype.Int4{Int32: -1, Valid: true},
	})
	if err == nil {
		m.usersCache.UpdateUserStatistics(
			follow.SubjectDid, 0, -1, 0, 0,
		)
	}

	// Finish transaction
	tx.Commit(ctx)
}

func (m *Manager) DeleteInteraction(uri string) {
	ctx := context.Background()

	m.interactionsToDeleteMutex.Lock()
	defer m.interactionsToDeleteMutex.Unlock()

	m.interactionsToDelete = append(m.interactionsToDelete, uri)

	if len(m.interactionsToDelete) >= InteractionsToDeleteBulkSize {
		// Copy buffer and exec bulk delete
		go func(uris []string) {
			deletedInteractions, err := m.queries.BulkDeleteInteractions(ctx, uris)
			if err != nil {
				log.Errorf("Error deleting interactions: %m", err)
			}

			// Update caches
			for _, interaction := range deletedInteractions {
				postAuthor, ok := m.postsCache.GetPostAuthor(interaction.PostUri)
				if ok {
					m.postsCache.DeleteInteraction(interaction.PostUri)
					m.usersCache.UpdateUserStatistics(
						postAuthor, 0, 0, 0, -1,
					)
				}
			}
		}(m.interactionsToDelete)

		// Clear buffer
		m.interactionsToDelete = make([]string, 0, InteractionsToDeleteBulkSize)
	}
}

func (m *Manager) DeletePost(uri string) {
	ctx := context.Background()

	// Delete from cache
	m.postsCache.DeletePost(uri)

	m.postsToDeleteMutex.Lock()
	defer m.postsToDeleteMutex.Unlock()

	m.postsToDelete = append(m.postsToDelete, uri)

	if len(m.postsToDelete) >= PostsToDeleteBulkSize {
		// Copy buffer and exec bulk delete
		go func(uris []string) {
			// Start transaction
			tx, err := m.dbConnection.Begin(ctx)
			if err != nil {
				log.Warningf("Error creating transaction: %v", err)
			}
			defer tx.Rollback(ctx) // Rollback on error
			qtx := m.queries.WithTx(tx)

			// Delete posts
			posts, err := qtx.BulkDeletePosts(ctx, uris)
			if err != nil {
				log.Errorf("Error deleting posts: %m", err)
				return
			}

			// Remove post from user statistics
			for _, post := range posts {
				err = qtx.AddUserPosts(ctx, db.AddUserPostsParams{
					Did:        post.AuthorDid,
					PostsCount: pgtype.Int4{Int32: -1, Valid: true},
				})
				if err == nil {
					// Update caches
					postInteractions := m.postsCache.GetPostInteractions(post.AuthorDid)
					m.usersCache.UpdateUserStatistics(
						post.AuthorDid, 0, 0, -1, -postInteractions,
					)
				}
			}

			// Finish transaction
			tx.Commit(ctx)
		}(
			m.postsToDelete,
		)
		// Clear buffer
		m.postsToDelete = make([]string, 0, PostsToDeleteBulkSize)
	}
}

func (m *Manager) DeleteUser(did string) {
	ctx := context.Background()

	// Delete user from DB
	if err := m.queries.DeleteUser(ctx, did); err != nil {
		log.Errorf("Error deleting user %s: %v", did, err)
		return
	}
	// Delete user's posts
	if err := m.queries.DeleteUserPosts(ctx, did); err != nil {
		log.Errorf("Error deleting posts for user %s: %v", did, err)
	}
	// Delete user's interactions
	if err := m.queries.DeleteUser(ctx, did); err != nil {
		log.Errorf("Error deleting user %s: %v", did, err)
	}

	// Delete user from cache
	m.usersCache.DeleteUser(did)
}

func (m *Manager) GetCursor(service string) int64 {
	state, _ := m.queries.GetSubscriptionState(
		context.Background(),
		service,
	)
	return state.Cursor // defaults to 0 if not in DB
}

func (m *Manager) GetOutdatedUserDids() []string {
	dids, err := m.queries.GetUserDidsToRefreshStatistics(context.Background())
	if err != nil {
		log.Errorf("Error getting user dids for update: %v", err)
	}
	return dids
}

func (m *Manager) GetTimeline(timelineName string, maxRank float64, limit int64) []models.Post {
	// Attempt to hit cache first
	timeline, ok := m.timelines[timelineName]
	if !ok {
		panic(fmt.Sprintf("Could not find timeline for feed: %s", timelineName))
	}
	posts := timeline.GetPosts(maxRank, limit)

	// Not found. Go to DB
	if int64(len(posts)) < limit {
		algorithm, ok := m.algorithms[timelineName]
		if !ok {
			panic(fmt.Sprintf("Could not find algorithm for feed: %s", timelineName))
		}
		posts = algorithm.GetPosts(m.queries, maxRank, limit)

		// Add to cache
		for _, post := range posts {
			timeline.AddPost(post)
		}
	}

	return posts
}

func (m *Manager) UpdateCursor(service string, cursor int64) {
	err := m.queries.UpdateSubscriptionStateCursor(
		context.Background(),
		db.UpdateSubscriptionStateCursorParams{
			Cursor:  cursor,
			Service: service,
		},
	)
	if err != nil {
		log.Errorf("Error updating cursor: %m", err)
	}
}

func (m *Manager) UpdateUser(updatedUser models.User) {
	// Update on cache
	m.usersCache.SetUserFollows(updatedUser.Did, updatedUser.FollowersCount, updatedUser.FollowsCount)

	// Update on DB
	err := m.queries.UpdateUser(
		context.Background(),
		db.UpdateUserParams{
			Did:            updatedUser.Did,
			Handle:         pgtype.Text{String: updatedUser.Handle, Valid: true},
			FollowersCount: pgtype.Int4{Int32: int32(updatedUser.FollowersCount), Valid: true},
			FollowsCount:   pgtype.Int4{Int32: int32(updatedUser.FollowsCount), Valid: true},
			PostsCount:     pgtype.Int4{Int32: int32(updatedUser.PostsCount), Valid: true},
			LastUpdate:     pgtype.Timestamp{Time: time.Now(), Valid: true},
		},
	)
	if err != nil {
		log.Errorf("Error updating user: %v", err)
	}
}

func (m *Manager) initializeAlgorithms() {
	for feedName, algorithm := range algorithms.ImplementedAlgorithms {
		m.algorithms[feedName] = algorithm
	}
}

func (m *Manager) initializeTimelines() {
	for feedName := range algorithms.ImplementedAlgorithms {
		m.timelines[feedName] = cache.NewTimeline(feedName, m.redisConnection)
	}
}

func (m *Manager) loadUsersCreated() {
	dids, err := m.queries.GetUserDids(context.Background())
	if err != nil {
		log.Error(err)
		dids = make([]string, 0)
	}

	for _, did := range dids {
		m.usersCreated.Store(did, true)
	}
}

func (m *Manager) loadUsersFollows() {
	batchSize := int64(1000)
	ctx := context.Background()

	totalUsers, err := m.queries.GetUsersCount(ctx)
	if err != nil {
		log.Errorf("Error getting users count: %v", err)
		return
	}

	for i := int64(0); i < totalUsers; i += batchSize {
		go func() {
			// Get batch of counts
			counts, err := m.queries.BatchGetUsersFollows(ctx, db.BatchGetUsersFollowsParams{
				Offset: int32(i),
				Limit:  int32(batchSize),
			})
			if err != nil {
				log.Errorf("Error getting users follows: %v", err)
				return
			}

			// Process batch
			for _, userCounts := range counts {
				m.usersCache.SetUserFollows(
					userCounts.Did,
					int64(userCounts.FollowersCount.Int32),
					int64(userCounts.FollowsCount.Int32),
				)
			}
		}()
	}
}
