package storage

import (
	"bsky/storage/algorithms"
	"bsky/storage/cache"
	db "bsky/storage/db/sqlc"
	"bsky/storage/models"
	"bsky/utils"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"os"
	"slices"
	"strings"
	"time"
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
}

func NewManager(dbConnection *pgxpool.Pool, redisConnection *redis.Client, persistFollows bool) *Manager {
	usersCacheExpiration := utils.IntFromString(
		os.Getenv("USERS_CACHE_EXPIRATION_MINUTES"), 43200,
	)
	postsCacheExpiration := utils.IntFromString(
		os.Getenv("POSTS_CACHE_EXPIRATION_MINUTES"), 1080,
	)

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
		if !post.ReplyRootID.Valid { // replies are not counted
			postInteractions := m.postsCache.GetPostInteractions(post.ID)
			m.usersCache.UpdateUserStatistics(post.AuthorID, 0, 0, -1, -postInteractions)
		}
	}
	// Delete from posts cache
	m.postsCache.DeletePosts(postIds)
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
	if err != nil {
		log.Errorf("Error creating interaction: %v", err)
		return
	}

	// Update caches
	if result.IsCreated {
		m.postsCache.AddInteraction(interaction.PostId)
		if postAuthorId, err := m.GetPostAuthorId(interaction.PostId); err == nil { // post author found
			m.usersCache.UpdateUserStatistics(
				postAuthorId, 0, 0, 0, 1,
			)
		}
	}
}

func (m *Manager) CreatePost(post models.Post) {
	ctx := context.Background()

	// Add post to corresponding timelines
	authorStatistics := m.usersCache.GetUserStatistics(post.AuthorId)
	go func() {
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
		log.Errorf("Error deleting post: %v", err)
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
			CreatedAt:      pgtype.Timestamp{Time: updatedUser.CreatedAt, Valid: true},
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
