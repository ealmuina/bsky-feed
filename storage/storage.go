package storage

import (
	"bsky/storage/algorithms"
	"bsky/storage/cache"
	db "bsky/storage/db/sqlc"
	"bsky/storage/models"
	"context"
	"fmt"
	"github.com/bluesky-social/indigo/lex/util"
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

type Manager struct {
	redisConnection *redis.Client
	dbConnection    *pgxpool.Pool
	queries         *db.Queries

	usersCache cache.UsersCache
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

		usersCache: cache.NewUsersCache(redisConnection),
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

func (m *Manager) CalculateUserEngagement(did string) float64 {
	engagementFactor, err := m.queries.CalculateUserEngagement(context.Background(), did)
	if err != nil {
		log.Infof("Error while calculating engagement factor for user %s: %v", did, err)
		return 0
	}
	return engagementFactor
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
		timeline.DeleteExpiredPosts(time.Now().Add(-7 * 24 * time.Hour))
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
	_ = qtx.AddUserFollows(ctx, db.AddUserFollowsParams{
		Did:          follow.AuthorDid,
		FollowsCount: pgtype.Int4{Int32: 1, Valid: true},
	})
	_ = qtx.AddUserFollowers(ctx, db.AddUserFollowersParams{
		Did:            follow.SubjectDid,
		FollowersCount: pgtype.Int4{Int32: 1, Valid: true},
	})

	// Finish transaction
	tx.Commit(ctx)

	// Update cache
	m.usersCache.UpdateUserFollowCounts(follow.AuthorDid, 1, 0)
	m.usersCache.UpdateUserFollowCounts(follow.SubjectDid, 0, 1)
}

func (m *Manager) CreateInteraction( // TODO Interaction model
	authorDid string,
	uri string,
	cid *util.LexLink,
	kind db.InteractionType,
	createdAt time.Time,
	postUri string,
) error {
	ctx := context.Background()

	m.interactionsToCreateMutex.Lock()
	defer m.interactionsToCreateMutex.Unlock()

	m.interactionsToCreate = append(
		m.interactionsToCreate,
		db.BulkCreateInteractionsParams{
			Uri:       uri,
			Cid:       cid.String(),
			Kind:      kind,
			AuthorDid: authorDid,
			PostUri:   postUri,
			CreatedAt: pgtype.Timestamp{Time: createdAt, Valid: true},
		},
	)
	if len(m.interactionsToCreate) >= InteractionsToCreateBulkSize {
		// Clone buffer and exec bulk insert
		go func(interactions []db.BulkCreateInteractionsParams) {
			if _, err := m.queries.BulkCreateInteractions(ctx, interactions); err != nil {
				log.Errorf("Error creating interactions: %v", err)
			}
		}(m.interactionsToCreate)
		// Clear buffer
		m.interactionsToCreate = make([]db.BulkCreateInteractionsParams, 0, InteractionsToCreateBulkSize)
	}

	return nil
}

func (m *Manager) CreatePost(post models.Post) {
	// Add post to corresponding timelines
	_, author := m.GetUser(post.AuthorDid)
	for timelineName, algorithm := range m.algorithms {
		if ok, reason := algorithm.AcceptsPost(post, author); ok {
			post.Reason = reason
			m.AddPostToTimeline(timelineName, post)
		}
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
			if _, err := m.queries.BulkCreatePosts(context.Background(), posts); err != nil {
				log.Errorf("Error creating posts: %v", err)
			}
		}(m.postsToCreate)
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
	_ = qtx.AddUserFollows(ctx, db.AddUserFollowsParams{
		Did:          follow.AuthorDid,
		FollowsCount: pgtype.Int4{Int32: -1, Valid: true},
	})
	_ = qtx.AddUserFollowers(ctx, db.AddUserFollowersParams{
		Did:            follow.SubjectDid,
		FollowersCount: pgtype.Int4{Int32: -1, Valid: true},
	})

	// Finish transaction
	tx.Commit(ctx)

	// Update cache
	m.usersCache.UpdateUserFollowCounts(follow.AuthorDid, -1, 0)
	m.usersCache.UpdateUserFollowCounts(follow.SubjectDid, 0, -1)
}

func (m *Manager) DeleteInteraction(uri string) {
	ctx := context.Background()

	m.interactionsToDeleteMutex.Lock()
	defer m.interactionsToDeleteMutex.Unlock()

	m.interactionsToDelete = append(m.interactionsToDelete, uri)

	if len(m.interactionsToDelete) >= InteractionsToDeleteBulkSize {
		// Copy buffer and exec bulk delete
		go func(uris []string) {
			if err := m.queries.BulkDeleteInteractions(ctx, uris); err != nil {
				log.Errorf("Error deleting interactions: %m", err)
			}
		}(m.interactionsToDelete)
		// Clear buffer
		m.interactionsToDelete = make([]string, 0, InteractionsToDeleteBulkSize)
	}
}

func (m *Manager) DeletePost(uri string) {
	ctx := context.Background()

	m.postsToDeleteMutex.Lock()
	defer m.postsToDeleteMutex.Unlock()

	m.postsToDelete = append(m.postsToDelete, uri)

	if len(m.postsToDelete) >= PostsToDeleteBulkSize {
		// Copy buffer and exec bulk delete
		go func(uris []string) {
			if err := m.queries.BulkDeletePosts(ctx, uris); err != nil {
				log.Errorf("Error deleting posts: %m", err)
			}
		}(m.postsToDelete)
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

func (m *Manager) GetUser(did string) (ok bool, user models.User) {
	// Check cache
	ok, cacheUser := m.usersCache.GetUser(did)
	if !ok {
		// Not in cache. Get it from DB
		dbUser, err := m.queries.GetUser(context.Background(), did)
		if err != nil {
			// Not in DB either
			return false, models.User{}
		}
		if !dbUser.FollowersCount.Valid || !dbUser.EngagementFactor.Valid {
			// No statistics from user in DB
			return false, models.User{}
		}
		// Fill cacheUser and store it in cache for future requests
		cacheUser = models.User{
			Did:              dbUser.Did,
			FollowersCount:   int64(dbUser.FollowersCount.Int32),
			EngagementFactor: dbUser.EngagementFactor.Float64,
		}
		m.usersCache.AddUser(cacheUser)
	}
	return true, cacheUser
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
	m.usersCache.AddUser(updatedUser)

	// Update on DB
	err := m.queries.UpdateUser(
		context.Background(),
		db.UpdateUserParams{
			Did:            updatedUser.Did,
			Handle:         pgtype.Text{String: updatedUser.Handle, Valid: true},
			FollowersCount: pgtype.Int4{Int32: int32(updatedUser.FollowersCount), Valid: true},
			FollowsCount:   pgtype.Int4{Int32: int32(updatedUser.FollowsCount), Valid: true},
			PostsCount:     pgtype.Int4{Int32: int32(updatedUser.PostsCount), Valid: true},
			EngagementFactor: pgtype.Float8{
				Float64: updatedUser.EngagementFactor,
				Valid:   updatedUser.EngagementFactor > 0,
			},
			LastUpdate: pgtype.Timestamp{Time: time.Now(), Valid: true},
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
