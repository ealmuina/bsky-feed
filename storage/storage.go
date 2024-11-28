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
	timelines  map[string]cache.Timeline
	algorithms map[string]algorithms.Algorithm
}

func NewManager(dbConnection *pgxpool.Pool, redisConnection *redis.Client, persistFollows bool) *Manager {
	storageManager := Manager{
		redisConnection: redisConnection,
		dbConnection:    dbConnection,
		queries:         db.New(dbConnection),
		persistFollows:  persistFollows,

		usersCache: cache.NewUsersCache(
			redisConnection,
			//30*24*time.Hour, // expire entries after 30 days
			5*time.Minute, // expire entries after 5 minutes
		),
		postsCache: cache.NewPostsCache(
			redisConnection,
			//7*24*time.Hour, // expire entries after 7 days
			5*time.Minute, // expire entries after 5 minutes
		),
		timelines:  make(map[string]cache.Timeline),
		algorithms: make(map[string]algorithms.Algorithm),
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
		m.executeTransaction(
			func(ctx context.Context, qtx *db.Queries) {
				// Create follow
				_, err := qtx.CreateFollow(ctx, db.CreateFollowParams{
					UriKey:    follow.UriKey,
					AuthorID:  follow.AuthorID,
					SubjectID: follow.SubjectID,
					CreatedAt: pgtype.Timestamp{Time: follow.CreatedAt, Valid: true},
				})
				if err != nil {
					if !strings.Contains(err.Error(), "no rows in result set") {
						log.Errorf("Error creating follow: %v", err)
					}
					return
				}

				// Update statistics
				m.refreshFollowStatistics(ctx, qtx, follow.AuthorID, follow.SubjectID, 1)
			},
		)
	} else {
		// Do not persist. Just update statistics
		m.refreshFollowStatistics(context.Background(), m.queries, follow.AuthorID, follow.SubjectID, 1)
	}
}

func (m *Manager) CreateInteraction(interaction models.Interaction) {
	m.executeTransaction(
		func(ctx context.Context, qtx *db.Queries) {
			// Create interaction
			_, err := qtx.CreateInteraction(ctx, db.CreateInteractionParams{
				UriKey:    interaction.UriKey,
				Kind:      int16(interaction.Kind),
				AuthorID:  interaction.AuthorId,
				PostID:    interaction.PostId,
				CreatedAt: pgtype.Timestamp{Time: interaction.CreatedAt, Valid: true},
			})
			if err != nil {
				if !strings.Contains(err.Error(), "no rows in result set") {
					log.Errorf("Error creating interaction: %v", err)
				}
				return
			}
		},
	)

	// Update caches
	m.postsCache.AddInteraction(interaction.PostId)
	if postAuthorId, err := m.GetPostAuthorId(interaction.PostId); err == nil { // post author found
		m.usersCache.UpdateUserStatistics(
			postAuthorId, 0, 0, 0, 1,
		)
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
		m.executeTransaction(
			func(ctx context.Context, qtx *db.Queries) {
				// Delete follow
				follow, err := qtx.DeleteFollow(ctx, db.DeleteFollowParams{
					UriKey:   identifier.UriKey,
					AuthorID: identifier.AuthorId,
				})
				if err != nil {
					if !strings.Contains(err.Error(), "no rows in result set") {
						log.Errorf("Error deleting follow: %v", err)
					}
					return
				}

				// Update user statistics
				m.refreshFollowStatistics(ctx, qtx, follow.AuthorID, follow.SubjectID, -1)
			})
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
	m.postsCache.DeleteInteraction(interaction.ID)
	if postAuthorId, err := m.GetPostAuthorId(interaction.PostID); err == nil { // post author found
		m.usersCache.UpdateUserStatistics(
			postAuthorId, 0, 0, 0, 1,
		)
	}
}

func (m *Manager) DeletePost(identifier models.Identifier) {
	var post db.DeletePostRow
	var postInteractions []db.GetPostInteractionsRow

	m.executeTransaction(
		func(ctx context.Context, qtx *db.Queries) {
			// Delete posts
			var err error
			post, err = m.queries.DeletePost(ctx, db.DeletePostParams{
				UriKey:   identifier.UriKey,
				AuthorID: identifier.AuthorId,
			})
			if err != nil {
				log.Errorf("Error deleting post: %v", err)
				return
			}

			// Remove post from user statistics
			err = qtx.AddUserPosts(ctx, db.AddUserPostsParams{
				ID:         post.AuthorID,
				PostsCount: pgtype.Int4{Int32: -1, Valid: true},
			})
			if err != nil {
				log.Errorf("Error removing post to user '%d': %v", post.AuthorID, err)
				return
			}

			// Remove corresponding interactions
			postInteractions, err = qtx.GetPostInteractions(ctx, post.ID)
			if err != nil {
				log.Errorf("Error getting post interactions: %v", err)
			}
			for _, interaction := range postInteractions {
				m.DeleteInteraction(models.Identifier{
					UriKey:   interaction.UriKey,
					AuthorId: interaction.AuthorID,
				})
			}
		},
	)

	// Update caches
	m.postsCache.DeletePost(post.ID)
	if len(postInteractions) > 0 {
		m.usersCache.UpdateUserStatistics(
			post.AuthorID, 0, 0, -1, int64(-len(postInteractions)),
		)
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
