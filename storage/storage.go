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
	"slices"
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

	usersCache cache.UsersCache
	postsCache cache.PostsCache
	timelines  map[string]cache.Timeline
	algorithms map[string]algorithms.Algorithm

	buffers      sync.Map
	mutexes      map[Event]*sync.Mutex
	usersCreated sync.Map
}

func NewManager(dbConnection *pgxpool.Pool, redisConnection *redis.Client) *Manager {
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

		buffers:      sync.Map{},
		mutexes:      mutexes,
		usersCreated: sync.Map{},
	}
	storageManager.loadUsersCreated()
	go storageManager.loadUsersFollows()
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
	deletedPosts, err := m.queries.DeleteOldPosts(ctx)
	if err != nil {
		log.Errorf("Error cleaning old posts: %v", err)
	}
	if err := m.queries.VacuumPosts(ctx); err != nil {
		log.Errorf("Error vacuuming posts table: %v", err)
	}
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
	postUris := make([]string, 0, len(deletedPosts))
	for _, deletedPost := range deletedPosts {
		postUris = append(postUris, deletedPost.Uri)

		// Discount from user statistics
		postInteractions := m.postsCache.GetPostInteractions(deletedPost.Uri)
		m.usersCache.UpdateUserStatistics(deletedPost.AuthorDid, 0, 0, -1, -postInteractions)
	}
	// Delete from posts cache
	m.postsCache.DeletePosts(postUris)
}

func (m *Manager) CreateFollow(follow models.Follow) {
	event := Event{EntityFollow, OperationCreate}
	mutex := m.mutexes[event]
	if mutex == nil {
		log.Errorf("Mutex for follows create buffer not initialized")
		return
	}
	mutex.Lock()
	defer mutex.Unlock()

	var buffer []db.BulkCreateFollowsParams
	b, ok := m.buffers.Load(event)
	if ok {
		buffer = b.([]db.BulkCreateFollowsParams)
	} else {
		buffer = make([]db.BulkCreateFollowsParams, 0)
	}

	buffer = append(
		buffer,
		db.BulkCreateFollowsParams{
			Uri:        follow.Uri,
			AuthorDid:  follow.AuthorDid,
			SubjectDid: follow.SubjectDid,
			CreatedAt:  pgtype.Timestamp{Time: follow.CreatedAt, Valid: true},
		},
	)
	m.buffers.Store(event, buffer)

	if len(buffer) >= BulkSize[event.Entity] {
		go m.executeTransaction(
			func(ctx context.Context, qtx *db.Queries) {
				// Create temporary table
				if err := qtx.CreateTempFollowsTable(ctx); err != nil {
					log.Infof("Error creating tmp_follows: %v", err)
					return
				}
				// Copy data to temporary table
				if _, err := qtx.BulkCreateFollows(context.Background(), buffer); err != nil {
					log.Errorf("Error creating follows: %v", err)
					return
				}
				// Move data from temporary table to follows
				createdFollows, err := qtx.InsertFromTempToFollows(ctx)
				if err != nil {
					log.Errorf("Error persisting follows: %v", err)
					return
				}

				// Add follow to users statistics
				for _, follow := range createdFollows {
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
				}
			},
		)

		// Clear buffer
		m.buffers.Store(event, make([]db.BulkCreateFollowsParams, 0, len(buffer)))
	}
}

func (m *Manager) CreateInteraction(interaction models.Interaction) error {
	event := Event{EntityInteraction, OperationCreate}
	mutex := m.mutexes[event]
	if mutex == nil {
		err := fmt.Errorf("mutex for interactions create buffer not initialized")
		log.Error(err)
		return err
	}
	mutex.Lock()
	defer mutex.Unlock()

	var buffer []db.BulkCreateInteractionsParams
	b, ok := m.buffers.Load(event)
	if ok {
		buffer = b.([]db.BulkCreateInteractionsParams)
	} else {
		buffer = make([]db.BulkCreateInteractionsParams, 0)
	}

	buffer = append(
		buffer,
		db.BulkCreateInteractionsParams{
			Uri:       interaction.Uri,
			Kind:      db.InteractionType(interaction.Kind),
			AuthorDid: interaction.AuthorDid,
			PostUri:   interaction.PostUri,
			CreatedAt: pgtype.Timestamp{Time: interaction.CreatedAt, Valid: true},
		},
	)
	m.buffers.Store(event.Entity, buffer)

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
				postAuthor, ok := m.postsCache.GetPostAuthor(interaction.PostUri)
				if ok {
					m.postsCache.AddInteraction(interaction.PostUri)
					m.usersCache.UpdateUserStatistics(
						postAuthor, 0, 0, 0, 1,
					)
				}
			}
		}()

		// Clear buffer
		m.buffers.Store(event, make([]db.BulkCreateInteractionsParams, 0, len(buffer)))
	}

	return nil
}

func (m *Manager) CreatePost(post models.Post) {
	// Add post to corresponding timelines
	authorStatistics := m.usersCache.GetUserStatistics(post.AuthorDid)
	go func() {
		for timelineName, algorithm := range m.algorithms {
			if ok, reason := algorithm.AcceptsPost(post, authorStatistics); ok {
				post.Reason = reason
				m.AddPostToTimeline(timelineName, post)
			}
		}
	}()

	// Store in cache (exclude replies)
	// *Done at this step so the post is available in cache as soon as possible
	// instead of waiting until bulk creation
	if post.ReplyRoot == "" {
		m.postsCache.AddPost(post)
	}

	// Store in DB
	event := Event{EntityPost, OperationCreate}
	mutex := m.mutexes[event]
	if mutex == nil {
		log.Errorf("Mutex for posts create buffer not initialized")
		return
	}
	mutex.Lock()
	defer mutex.Unlock()

	var buffer []db.BulkCreatePostsParams
	b, ok := m.buffers.Load(event)
	if ok {
		buffer = b.([]db.BulkCreatePostsParams)
	} else {
		buffer = make([]db.BulkCreatePostsParams, 0)
	}

	buffer = append(
		buffer,
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
	m.buffers.Store(event, buffer)

	if len(buffer) >= BulkSize[event.Entity] {
		go m.executeTransaction(
			func(ctx context.Context, qtx *db.Queries) {
				// Create temporary table
				if err := qtx.CreateTempPostsTable(ctx); err != nil {
					log.Infof("Error creating tmp_posts: %v", err)
					return
				}
				// Copy data to temporary table
				if _, err := qtx.BulkCreatePosts(context.Background(), buffer); err != nil {
					log.Errorf("Error creating posts: %v", err)
					return
				}
				// Move data from temporary table to posts
				createdPosts, err := qtx.InsertFromTempToPosts(ctx)
				if err != nil {
					log.Errorf("Error persisting posts: %v", err)
					return
				}

				// Add post to user statistics
				for _, post := range createdPosts {
					err = qtx.AddUserPosts(ctx, db.AddUserPostsParams{
						Did:        post.AuthorDid,
						PostsCount: pgtype.Int4{Int32: 1, Valid: true},
					})
					if err == nil {
						// Update cache (exclude replies)
						if !post.ReplyRoot.Valid {
							m.usersCache.UpdateUserStatistics(
								post.AuthorDid, 0, 0, 1, 0,
							)
						}
					}
				}
			},
		)

		// Clear buffer
		m.buffers.Store(event, make([]db.BulkCreatePostsParams, 0, len(buffer)))
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
	event := Event{EntityFollow, OperationDelete}
	mutex := m.mutexes[event]
	if mutex == nil {
		log.Errorf("Mutex for follows delete buffer not initialized")
		return
	}
	mutex.Lock()
	defer mutex.Unlock()

	var buffer []string
	b, ok := m.buffers.Load(event)
	if ok {
		buffer = b.([]string)
	} else {
		buffer = make([]string, 0)
	}

	buffer = append(buffer, uri)
	m.buffers.Store(event, buffer)

	if len(buffer) >= BulkSize[event.Entity] {
		go m.executeTransaction(
			func(ctx context.Context, qtx *db.Queries) {
				// Delete follows
				deletedFollows, err := qtx.BulkDeleteFollows(ctx, buffer)
				if err != nil {
					log.Infof("Error deleting follows: %v", err)
					return
				}

				// Remove follow from users statistics
				for _, follow := range deletedFollows {
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
				}
			},
		)

		// Clear buffer
		m.buffers.Store(event, make([]string, 0, len(buffer)))
	}
}

func (m *Manager) DeleteInteraction(uri string) {
	ctx := context.Background()

	event := Event{EntityInteraction, OperationDelete}
	mutex := m.mutexes[event]
	if mutex == nil {
		log.Errorf("Mutex for interactions delete buffer not initialized")
		return
	}
	mutex.Lock()
	defer mutex.Unlock()

	var buffer []string
	b, ok := m.buffers.Load(event)
	if ok {
		buffer = b.([]string)
	} else {
		buffer = make([]string, 0)
	}

	buffer = append(buffer, uri)
	m.buffers.Store(event, buffer)

	if len(buffer) >= BulkSize[event.Entity] {
		go func() {
			deletedInteractions, err := m.queries.BulkDeleteInteractions(ctx, buffer)
			if err != nil {
				log.Errorf("Error deleting interactions: %v", err)
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
		}()

		// Clear buffer
		m.buffers.Store(event, make([]string, 0, len(buffer)))
	}
}

func (m *Manager) DeletePost(uri string) {
	event := Event{EntityPost, OperationDelete}
	mutex := m.mutexes[event]
	if mutex == nil {
		log.Errorf("Mutex for posts delete buffer not initialized")
		return
	}
	mutex.Lock()
	defer mutex.Unlock()

	var buffer []string
	b, ok := m.buffers.Load(event)
	if ok {
		buffer = b.([]string)
	} else {
		buffer = make([]string, 0)
	}

	buffer = append(buffer, uri)
	m.buffers.Store(event, buffer)

	if len(buffer) >= BulkSize[event.Entity] {
		go m.executeTransaction(
			func(ctx context.Context, qtx *db.Queries) {
				// Delete posts
				posts, err := qtx.BulkDeletePosts(ctx, buffer)
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
						deleted := m.postsCache.DeletePost(uri)
						if deleted {
							postInteractions := m.postsCache.GetPostInteractions(post.AuthorDid)
							m.usersCache.UpdateUserStatistics(
								post.AuthorDid, 0, 0, -1, -postInteractions,
							)
						}
					}
				}
			},
		)

		// Clear buffer
		m.buffers.Store(event, make([]string, 0, len(buffer)))
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
		dbPosts := algorithm.GetPosts(m.queries, maxRank, limit)
		for _, post := range dbPosts {
			if !slices.ContainsFunc(posts, func(post models.Post) bool {
				for _, p := range posts {
					if p.Uri == post.Uri {
						return true
					}
				}
				return false
			}) {
				posts = append(posts, post)
				// Add to cache
				timeline.AddPost(post)
			}
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
	counts, err := m.queries.GetUsersFollows(context.Background())
	if err != nil {
		log.Errorf("Error getting users follows: %v", err)
		return
	}

	for _, userCounts := range counts {
		m.usersCache.SetUserFollows(
			userCounts.Did,
			int64(userCounts.FollowersCount.Int32),
			int64(userCounts.FollowsCount.Int32),
		)
	}
}
