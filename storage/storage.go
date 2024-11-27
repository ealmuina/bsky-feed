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
	EntityUser               = "user"
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
		Event{EntityUser, OperationUpdate}:        {},
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
			//7*24*time.Hour, // expire entries after 7 days
			5*time.Minute, // expire entries after 5 minutes
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

func (m *Manager) CleanOldData(persistentDb bool) {
	// Clean DB
	ctx := context.Background()
	if !persistentDb {
		if err := m.queries.DeleteOldInteractions(ctx); err != nil {
			log.Errorf("Error cleaning old interactions: %v", err)
		}
		if err := m.queries.VacuumInteractions(ctx); err != nil {
			log.Errorf("Error vacuuming interactions: %v", err)
		}
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
	postIds := make([]int64, 0, len(oldPosts))
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
	if m.persistFollows {
		event := Event{EntityFollow, OperationCreate}
		mutex := m.mutexes[event]
		if mutex == nil {
			log.Errorf("Mutex for follows create buffer not initialized")
			return
		}
		mutex.Lock()
		defer mutex.Unlock()

		buffer := getBuffer(event, &m.buffers, make([]db.BulkCreateFollowsParams, 0))
		buffer = append(
			buffer,
			db.BulkCreateFollowsParams{
				UriKey:    follow.UriKey,
				AuthorID:  follow.AuthorID,
				SubjectID: follow.SubjectID,
				CreatedAt: pgtype.Timestamp{Time: follow.CreatedAt, Valid: true},
			},
		)
		m.buffers.Store(event, buffer)

		if len(buffer) >= BulkSize[event.Entity] {
			go func() {
				var createdFollows []db.InsertFromTempToFollowsRow

				m.executeTransaction(
					func(ctx context.Context, qtx *db.Queries) {
						// Create temporary table
						err := qtx.CreateTempFollowsTable(ctx)
						if err != nil {
							log.Infof("Error creating tmp_follows: %v", err)
							return
						}
						// Copy data to temporary table
						if _, err := qtx.BulkCreateFollows(ctx, buffer); err != nil {
							log.Errorf("Error creating follows: %v", err)
							return
						}
						// Move data from temporary table to follows
						createdFollows, err = qtx.InsertFromTempToFollows(ctx)
						if err != nil {
							log.Errorf("Error persisting follows: %v", err)
							return
						}

						// Update statistics
						usersMutex := m.mutexes[Event{EntityUser, OperationUpdate}]
						if usersMutex == nil {
							log.Errorf("Mutex for users update not initialized")
							return
						}
						mutex.Lock()
						for _, follow := range createdFollows {
							m.refreshFollowStatistics(ctx, qtx, follow.AuthorID, follow.SubjectID, 1)
						}
						mutex.Unlock()
					},
				)
			}()

			// Clear buffer
			m.buffers.Store(event, make([]db.BulkCreateFollowsParams, 0, len(buffer)))
		}
	} else {
		// Do not persist. Just update statistics
		m.refreshFollowStatistics(context.Background(), m.queries, follow.AuthorID, follow.SubjectID, 1)
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
			UriKey:    interaction.UriKey,
			Kind:      int16(interaction.Kind),
			AuthorID:  interaction.AuthorId,
			PostID:    interaction.PostId,
			CreatedAt: pgtype.Timestamp{Time: interaction.CreatedAt, Valid: true},
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
				m.postsCache.AddInteraction(interaction.PostID)
				if postAuthorId, err := m.GetPostAuthorId(interaction.PostID); err == nil { // post author found
					m.usersCache.UpdateUserStatistics(
						postAuthorId, 0, 0, 0, 1,
					)
				}
			}
		}()

		// Clear buffer
		m.buffers.Store(event, make([]db.BulkCreateInteractionsParams, 0, len(buffer)))
	}
}

func (m *Manager) CreatePost(post models.Post) {
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

	m.executeTransaction(
		func(ctx context.Context, qtx *db.Queries) {
			upsertResult, err := qtx.UpsertPost(ctx, db.UpsertPostParams{
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

			if upsertResult.IsCreated {
				m.postsCache.AddPost(post)

				// Add post to user statistics
				err = qtx.AddUserPosts(ctx, db.AddUserPostsParams{
					ID:         post.AuthorId,
					PostsCount: pgtype.Int4{Int32: 1, Valid: true},
				})
				if err != nil {
					log.Errorf("Error adding post to user '%d': %v", post.AuthorId, err)
				}
			}
		},
	)

	// Update cache (exclude replies)
	if post.ReplyRootId == 0 {
		m.usersCache.UpdateUserStatistics(
			post.AuthorId, 0, 0, 1, 0,
		)
	}
}

func (m *Manager) DeleteFollow(identifier models.Identifier) {
	ctx := context.Background()

	if m.persistFollows {
		event := Event{EntityFollow, OperationDelete}
		mutex := m.mutexes[event]
		if mutex == nil {
			log.Errorf("Mutex for follows delete buffer not initialized")
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

				m.executeTransaction(
					func(ctx context.Context, qtx *db.Queries) {
						deletedFollows, err := qtx.BulkDeleteFollows(ctx, db.BulkDeleteFollowsParams{
							UriKeys:   uriKeys,
							AuthorIds: authorIds,
						})
						if err != nil {
							log.Errorf("Error deleting follows: %v", err)
							return
						}

						// Update user statistics
						usersMutex := m.mutexes[Event{EntityUser, OperationUpdate}]
						if usersMutex == nil {
							log.Errorf("Mutex for users update not initialized")
							return
						}
						mutex.Lock()
						for _, follow := range deletedFollows {
							m.refreshFollowStatistics(ctx, qtx, follow.AuthorID, follow.SubjectID, -1)
						}
						mutex.Unlock()
					})
			}()

			// Clear buffer
			m.buffers.Store(event, make([]models.Identifier, 0, len(buffer)))
		}
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
				m.postsCache.DeleteInteraction(interaction.ID)
				if postAuthorId, err := m.GetPostAuthorId(interaction.PostID); err == nil { // post author found
					m.usersCache.UpdateUserStatistics(
						postAuthorId, 0, 0, 0, 1,
					)
				}
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

					// Remove post from user statistics
					for _, post := range deletedPosts {
						err := qtx.AddUserPosts(ctx, db.AddUserPostsParams{
							ID:         post.AuthorID,
							PostsCount: pgtype.Int4{Int32: -1, Valid: true},
						})
						if err == nil {
							// Update caches
							m.postsCache.DeletePost(post.ID)
							postInteractions := m.postsCache.GetPostInteractions(post.ID)
							if postInteractions > 0 {
								m.usersCache.UpdateUserStatistics(
									post.AuthorID, 0, 0, -1, -postInteractions,
								)
							}
						}
					}
				},
			)

			// Remove corresponding interactions
			for _, post := range deletedPosts {
				postInteractions, err := m.queries.GetPostInteractions(ctx, post.ID)
				if err != nil {
					log.Errorf("Error getting post interactions: %v", err)
				}
				for _, interaction := range postInteractions {
					m.DeleteInteraction(models.Identifier{
						UriKey:   interaction.UriKey,
						AuthorId: interaction.AuthorID,
					})
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

	// Delete posts
	userPosts, err := m.queries.GetUserPosts(ctx, id)
	if err != nil {
		log.Errorf("Error getting user '%s' posts: %v", did, err)
	}
	for _, post := range userPosts {
		m.DeletePost(models.Identifier{
			UriKey:   post.UriKey,
			AuthorId: post.AuthorID,
		})
	}
	// Delete interactions
	userInteractions, err := m.queries.GetUserInteractions(ctx, id)
	if err != nil {
		log.Errorf("Error getting user '%s' interactions: %v", did, err)
	}
	for _, interaction := range userInteractions {
		m.DeleteFollow(models.Identifier{
			UriKey:   interaction.UriKey,
			AuthorId: interaction.AuthorID,
		})
	}
	if m.persistFollows {
		// Delete follows "touching" the user (follower or followed)
		userFollows, err := m.queries.GetFollowsTouchingUser(ctx, id)
		if err != nil {
			log.Errorf("Error getting user '%s' follows: %v", did, err)
		}
		for _, follow := range userFollows {
			m.DeleteFollow(models.Identifier{
				UriKey:   follow.UriKey,
				AuthorId: follow.AuthorID,
			})
		}
	}

	// Delete user from DB
	if err := m.queries.DeleteUser(ctx, id); err != nil {
		log.Errorf("Error deleting user %s: %v", did, err)
		return
	}

	// Delete user from cache
	m.usersCache.DeleteUser(id)
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

func (m *Manager) GetOutdatedUserDids() []string {
	dids, err := m.queries.GetUserDidsToRefreshStatistics(context.Background())
	if err != nil {
		log.Errorf("Error getting user dids for update: %v", err)
	}
	return dids
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

	if upsertResult.IsCreated {
		// Add post to user statistics
		_ = m.queries.AddUserPosts(ctx, db.AddUserPostsParams{
			ID:         authorId,
			PostsCount: pgtype.Int4{Int32: 1, Valid: true},
		})
	}
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

func (m *Manager) initializeTimelines() {
	for feedName := range algorithms.ImplementedAlgorithms {
		m.timelines[feedName] = cache.NewTimeline(feedName, m.redisConnection)
	}
}

func (m *Manager) refreshFollowStatistics(
	ctx context.Context,
	queries *db.Queries,
	authorId, subjectId, delta int32,
) {
	err := queries.ApplyFollowToUsers(ctx, db.ApplyFollowToUsersParams{
		AuthorID:  authorId,
		SubjectID: subjectId,
		Delta:     delta,
	})
	if err == nil {
		m.usersCache.UpdateUserStatistics(
			authorId, int64(delta), 0, 0, 0,
		)
		m.usersCache.UpdateUserStatistics(
			subjectId, 0, int64(delta), 0, 0,
		)
	}
}
