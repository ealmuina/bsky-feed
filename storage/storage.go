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

type Entity string
type Operation string

const (
	EntityPost        Entity = "post"
	EntityInteraction        = "interaction"
	EntityFollow             = "follow"
)

const (
	OperationCreate Operation = "create"
	OperationUpdate           = "update"
	OperationDelete           = "delete"
)

type Event struct {
	Entity    Entity
	Operation Operation
}

var BulkSize = map[Entity]int{
	EntityPost:        100,
	EntityInteraction: 100,
	EntityFollow:      100,
}

type Manager struct {
	redisConnection *redis.Client
	dbConnection    *pgxpool.Pool
	queries         *db.Queries
	persistFollows  bool

	usersCache cache.UsersCache
	postsCache cache.PostsCache
	timelines  map[string]cache.Timeline
	algorithms map[string]algorithms.Algorithm

	buffers sync.Map
	mutexes map[Event]*sync.Mutex
}

func getBuffer[T any](event Event, buffers *sync.Map, defaultBuffer []T) (buffer []T) {
	b, ok := buffers.Load(event)
	if ok {
		buffer = b.([]T)
	} else {
		buffer = defaultBuffer
	}
	return buffer
}

func NewManager(dbConnection *pgxpool.Pool, redisConnection *redis.Client, persistFollows bool) *Manager {
	mutexes := map[Event]*sync.Mutex{
		Event{EntityPost, OperationCreate}:        {},
		Event{EntityPost, OperationDelete}:        {},
		Event{EntityInteraction, OperationCreate}: {},
		Event{EntityInteraction, OperationDelete}: {},
		Event{EntityFollow, OperationCreate}:      {},
		Event{EntityFollow, OperationDelete}:      {},
	}

	storageManager := Manager{
		redisConnection: redisConnection,
		dbConnection:    dbConnection,
		queries:         db.New(dbConnection),
		persistFollows:  persistFollows,

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

		buffers: sync.Map{},
		mutexes: mutexes,
	}
	storageManager.initializeTimelines()
	storageManager.initializeAlgorithms()
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

func (m *Manager) CleanOldData() {
	// Clean DB
	ctx := context.Background()
	if err := m.queries.DeleteOldInteractions(ctx); err != nil {
		log.Errorf("Error cleaning old interactions: %v", err)
	}
	if err := m.queries.VacuumInteractions(ctx); err != nil {
		log.Errorf("Error vacuuming interactions: %v", err)
	}

	// Clean timelines
	for _, timeline := range m.timelines {
		timeline.DeleteExpiredPosts(time.Now().Add(-3 * 24 * time.Hour)) // Timelines lifespan of 3 days
	}

	// Clean caches
	oldPosts, err := m.queries.GetOldPosts(ctx)
	if err != nil {
		log.Errorf("Error retrieving old posts: %v", err)
	}
	postIds := make([]int32, 0, len(oldPosts))
	for _, post := range oldPosts {
		postIds = append(postIds, post.ID)

		// Discount from user statistics
		postInteractions := m.postsCache.GetPostInteractions(post.ID)
		m.usersCache.UpdateUserStatistics(post.AuthorID, 0, 0, -1, -postInteractions)
	}
	// Delete from posts cache
	m.postsCache.DeletePosts(postIds)
}

func (m *Manager) CreateFollow(follow models.Follow) {
	ctx := context.Background()

	if m.persistFollows {
		// Set user follow on DB
		err := m.queries.SetUserFollow(ctx, db.SetUserFollowParams{
			ID:        follow.AuthorID,
			Rkey:      follow.UriKey,
			SubjectID: follow.SubjectID,
		})
		if err != nil {
			log.Errorf("Error setting follow: %v", err)
			return
		}
	}

	// Add follow to user statistics
	err := m.queries.AddUserFollows(ctx, db.AddUserFollowsParams{
		ID:           follow.AuthorID,
		FollowsCount: pgtype.Int4{Int32: 1, Valid: true},
	})
	if err == nil {
		m.usersCache.UpdateUserStatistics(
			follow.AuthorID, 1, 0, 0, 0,
		)
	}
	err = m.queries.AddUserFollowers(ctx, db.AddUserFollowersParams{
		ID:             follow.SubjectID,
		FollowersCount: pgtype.Int4{Int32: 1, Valid: true},
	})
	if err == nil {
		m.usersCache.UpdateUserStatistics(
			follow.SubjectID, 0, 1, 0, 0,
		)
	}
}

func (m *Manager) CreateInteraction(interaction models.Interaction) {
	event := Event{EntityInteraction, OperationCreate}
	mutex := m.mutexes[event]
	if mutex == nil {
		log.Errorf("Mutex for interactions create buffer not initialized")
		return
	}
	mutex.Lock()
	defer mutex.Unlock()

	buffer := getBuffer(event, &m.buffers, make([]db.BulkCreateInteractionsParams, 0))
	buffer = append(
		buffer,
		db.BulkCreateInteractionsParams{
			UriKey:       interaction.UriKey,
			Kind:         int16(interaction.Kind),
			AuthorID:     interaction.AuthorID,
			PostUriKey:   interaction.PostUriKey,
			PostAuthorID: interaction.PostAuthorId,
			CreatedAt:    pgtype.Timestamp{Time: interaction.CreatedAt, Valid: true},
		},
	)
	m.buffers.Store(event, buffer)

	if len(buffer) >= BulkSize[event.Entity] {
		go func() {
			var createdInteractions []db.InsertFromTempToInteractionsRow

			m.executeTransaction(
				func(ctx context.Context, qtx *db.Queries) {
					// Create temporary table
					err := qtx.CreateTempInteractionsTable(ctx)
					if err != nil {
						log.Infof("Error creating tmp_interactions: %v", err)
						return
					}
					// Copy data to temporary table
					if _, err := qtx.BulkCreateInteractions(ctx, buffer); err != nil {
						log.Errorf("Error creating interactions: %v", err)
						return
					}
					// Move data from temporary table to interactions
					createdInteractions, err = qtx.InsertFromTempToInteractions(ctx)
					if err != nil {
						log.Errorf("Error persisting interactions: %v", err)
						return
					}
				},
			)

			// Update caches
			for _, interaction := range createdInteractions {
				m.postsCache.AddInteraction(interaction.PostAuthorID, interaction.PostUriKey)
				m.usersCache.UpdateUserStatistics(
					interaction.PostAuthorID, 0, 0, 0, 1,
				)
			}
		}()

		// Clear buffer
		m.buffers.Store(event, make([]db.BulkCreateInteractionsParams, 0, len(buffer)))
	}
}

func (m *Manager) CreatePost(post models.Post) {
	ctx := context.Background()

	// Add post to corresponding timelines
	authorStatistics := m.usersCache.GetUserStatistics(post.AuthorId)
	go func() {
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

	// Store in DB
	event := Event{EntityPost, OperationCreate}
	mutex := m.mutexes[event]
	if mutex == nil {
		log.Errorf("Mutex for posts create buffer not initialized")
		return
	}
	mutex.Lock()
	defer mutex.Unlock()

	buffer := getBuffer(event, &m.buffers, make([]db.BulkCreatePostsParams, 0))
	buffer = append(
		buffer,
		db.BulkCreatePostsParams{
			UriKey:      post.UriKey,
			AuthorID:    post.AuthorId,
			ReplyParent: post.ReplyParent,
			ReplyRoot:   post.ReplyRoot,
			CreatedAt:   pgtype.Timestamp{Time: post.CreatedAt, Valid: true},
			Language:    pgtype.Text{String: post.Language, Valid: post.Language != ""},
		},
	)
	m.buffers.Store(event, buffer)

	if len(buffer) >= BulkSize[event.Entity] {
		go func() {
			var createdPosts []db.InsertFromTempToPostsRow

			m.executeTransaction(
				func(ctx context.Context, qtx *db.Queries) {
					// Create temporary table
					err := qtx.CreateTempPostsTable(ctx)
					if err != nil {
						log.Infof("Error creating tmp_posts: %v", err)
						return
					}
					// Copy data to temporary table
					if _, err := qtx.BulkCreatePosts(context.Background(), buffer); err != nil {
						log.Errorf("Error creating posts: %v", err)
						return
					}
					// Move data from temporary table to posts
					createdPosts, err = qtx.InsertFromTempToPosts(ctx)
					if err != nil {
						log.Errorf("Error persisting posts: %v", err)
						return
					}
				},
			)

			// Add post to user statistics
			for _, post := range createdPosts {
				err := m.queries.AddUserPosts(ctx, db.AddUserPostsParams{
					ID:         post.AuthorID,
					PostsCount: pgtype.Int4{Int32: 1, Valid: true},
				})
				if err == nil {
					// Update cache (exclude replies)
					if post.ReplyRoot == nil {
						m.usersCache.UpdateUserStatistics(
							post.AuthorID, 0, 0, 1, 0,
						)
					}
				}
			}
		}()

		// Clear buffer
		m.buffers.Store(event, make([]db.BulkCreatePostsParams, 0, len(buffer)))
	}
}

func (m *Manager) DeleteFollow(identifier models.Identifier) {
	ctx := context.Background()

	if m.persistFollows {
		// Delete from DB
		subjectIdBytes, err := m.queries.RemoveUserFollow(ctx, db.RemoveUserFollowParams{
			ID:   identifier.AuthorId,
			Rkey: identifier.UriKey,
		})
		if err != nil {
			log.Errorf("Error removing follow: %v", err)
			return
		}
		if subjectIdBytes == nil {
			// Follow not found
			return
		}
		subjectId := int32(subjectIdBytes.(float64))

		// Discount from followed statistics
		err = m.queries.AddUserFollowers(ctx, db.AddUserFollowersParams{
			ID:             subjectId,
			FollowersCount: pgtype.Int4{Int32: -1, Valid: true},
		})
		if err == nil {
			m.usersCache.UpdateUserStatistics(
				subjectId, 0, -1, 0, 0,
			)
		}
	}

	// Discount from follower statistics
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

func (m *Manager) DeleteInteraction(identifier models.Identifier) {
	ctx := context.Background()

	event := Event{EntityInteraction, OperationDelete}
	mutex := m.mutexes[event]
	if mutex == nil {
		log.Errorf("Mutex for interactions delete buffer not initialized")
		return
	}
	mutex.Lock()
	defer mutex.Unlock()

	buffer := getBuffer(event, &m.buffers, make([]models.Identifier, 0))
	buffer = append(buffer, identifier)
	m.buffers.Store(event, buffer)

	if len(buffer) >= BulkSize[event.Entity] {
		go func() {
			uriKeys := make([]string, len(buffer))
			authorIds := make([]int32, len(buffer))

			for i, identifier := range buffer {
				uriKeys[i] = identifier.UriKey
				authorIds[i] = identifier.AuthorId
			}

			deletedInteractions, err := m.queries.BulkDeleteInteractions(ctx, db.BulkDeleteInteractionsParams{
				UriKeys:   uriKeys,
				AuthorIds: authorIds,
			})
			if err != nil {
				log.Errorf("Error deleting interactions: %v", err)
			}

			// Update caches
			for _, interaction := range deletedInteractions {
				m.postsCache.DeleteInteraction(interaction.PostAuthorID, interaction.PostUriKey)
				m.usersCache.UpdateUserStatistics(
					interaction.PostAuthorID, 0, 0, 0, -1,
				)
			}
		}()

		// Clear buffer
		m.buffers.Store(event, make([]models.Identifier, 0, len(buffer)))
	}
}

func (m *Manager) DeletePost(identifier models.Identifier) {
	ctx := context.Background()

	event := Event{EntityPost, OperationDelete}
	mutex := m.mutexes[event]
	if mutex == nil {
		log.Errorf("Mutex for posts delete buffer not initialized")
		return
	}
	mutex.Lock()
	defer mutex.Unlock()

	buffer := getBuffer(event, &m.buffers, make([]models.Identifier, 0))
	buffer = append(buffer, identifier)
	m.buffers.Store(event, buffer)

	if len(buffer) >= BulkSize[event.Entity] {
		go func() {
			var deletedPosts []db.BulkDeletePostsRow

			uriKeys := make([]string, len(buffer))
			authorIds := make([]int32, len(buffer))

			for i, identifier := range buffer {
				uriKeys[i] = identifier.UriKey
				authorIds[i] = identifier.AuthorId
			}

			m.executeTransaction(
				func(ctx context.Context, qtx *db.Queries) {
					// Delete posts
					var err error
					deletedPosts, err = qtx.BulkDeletePosts(ctx, db.BulkDeletePostsParams{
						UriKeys:   uriKeys,
						AuthorIds: authorIds,
					})
					if err != nil {
						log.Errorf("Error deleting posts: %m", err)
						return
					}
				},
			)

			// Remove post from user statistics
			for _, post := range deletedPosts {
				err := m.queries.AddUserPosts(ctx, db.AddUserPostsParams{
					ID:         post.AuthorID,
					PostsCount: pgtype.Int4{Int32: -1, Valid: true},
				})
				if err == nil {
					// Update caches
					deleted := m.postsCache.DeletePost(post.ID)
					if deleted {
						postInteractions := m.postsCache.GetPostInteractions(post.AuthorID)
						m.usersCache.UpdateUserStatistics(
							post.AuthorID, 0, 0, -1, -postInteractions,
						)
					}
				}
			}
		}()

		// Clear buffer
		m.buffers.Store(event, make([]models.Identifier, 0, len(buffer)))
	}
}

func (m *Manager) DeleteUser(did string) {
	ctx := context.Background()
	id, ok := m.usersCache.UserDidToId(did)
	if !ok {
		return
	}

	// Delete user from DB
	if err := m.queries.DeleteUser(ctx, id); err != nil {
		log.Errorf("Error deleting user %s: %v", did, err)
		return
	}
	// Delete user's posts
	if err := m.queries.DeleteUserPosts(ctx, id); err != nil {
		log.Errorf("Error deleting posts for user %s: %v", did, err)
	}
	// Delete user's interactions
	if err := m.queries.DeleteUser(ctx, id); err != nil {
		log.Errorf("Error deleting user %s: %v", did, err)
	}

	// Delete user from cache
	m.usersCache.DeleteUser(id)
}

func (m *Manager) GetCursor(service string) int64 {
	state, _ := m.queries.GetSubscriptionState(
		context.Background(),
		service,
	)
	return state.Cursor // defaults to 0 if not in DB
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

func (m *Manager) GetOutdatedUserDids() []string {
	dids, err := m.queries.GetUserDidsToRefreshStatistics(context.Background())
	if err != nil {
		log.Errorf("Error getting user dids for update: %v", err)
	}
	return dids
}

func (m *Manager) GetTimeline(timelineName string, maxRank float64, limit int64) []models.TimelineEntry {
	timeline, ok := m.timelines[timelineName]
	if !ok {
		panic(fmt.Sprintf("Could not find timeline for feed: %s", timelineName))
	}
	return timeline.GetPosts(maxRank, limit)
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
	m.usersCache.SetUserFollows(updatedUser.ID, updatedUser.FollowersCount, updatedUser.FollowsCount)

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
	tx.Commit(ctx)
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
