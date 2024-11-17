package storage

import (
	"bsky/storage/algorithms"
	"bsky/storage/cache"
	"bsky/storage/db/models"
	"bsky/storage/db/queries"
	"bsky/storage/utils"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/scylladb/gocqlx/v3"
	log "github.com/sirupsen/logrus"
	"slices"
	"sync"
	"time"
)

type Manager struct {
	redisConnection *redis.Client
	dbSession       *gocqlx.Session

	usersCache cache.UsersCache
	postsCache cache.PostsCache
	timelines  map[string]cache.Timeline
	algorithms map[string]algorithms.Algorithm

	usersCreated sync.Map
}

func NewManager(dbSession *gocqlx.Session, redisConnection *redis.Client) *Manager {
	storageManager := Manager{
		redisConnection: redisConnection,
		dbSession:       dbSession,

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

		usersCreated: sync.Map{},
	}
	storageManager.loadUsersCreated()
	storageManager.initializeTimelines()
	storageManager.initializeAlgorithms()
	return &storageManager
}

func (m *Manager) AddPostToTimeline(timelineName string, post models.PostsStruct, reason map[string]string) {
	timeline, ok := m.timelines[timelineName]
	if ok {
		timeline.AddPost(post, reason)
	} else {
		log.Errorf("Could not find timeline for feed name: %s", timelineName)
	}
}

func (m *Manager) CleanOldData() {
	// Clean DB
	deletedPosts, err := queries.DeleteOldPosts(m.dbSession)
	if err != nil {
		log.Errorf("Error cleaning old posts: %v", err)
	}
	if err := queries.DeleteOldInteractions(m.dbSession); err != nil {
		log.Errorf("Error cleaning old interactions: %v", err)
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

func (m *Manager) CreateFollow(follow models.FollowsStruct) {
	// Create follow
	if err := queries.CreateFollow(m.dbSession, follow); err != nil {
		log.Errorf("Error creating follow: %v", err)
		return
	}

	// Add follow to users statistics
	if err := queries.ApplyFollowToUsers(m.dbSession, follow, 1); err != nil {
		log.Errorf("Error adding follow to users: %v", err)
		return
	}
	m.usersCache.UpdateUserStatistics(
		follow.AuthorDid, 1, 0, 0, 0,
	)
	m.usersCache.UpdateUserStatistics(
		follow.SubjectDid, 0, 1, 0, 0,
	)
}

func (m *Manager) CreateInteraction(interaction models.InteractionsStruct) error {
	// Create interaction
	if err := queries.CreateInteraction(m.dbSession, interaction); err != nil {
		log.Errorf("Error creating interaction: %v", err)
		return err
	}

	// Update caches
	postAuthor, ok := m.postsCache.GetPostAuthor(interaction.PostUri)
	if ok {
		m.postsCache.AddInteraction(interaction.PostUri)
		m.usersCache.UpdateUserStatistics(
			postAuthor, 0, 0, 0, 1,
		)
	}

	return nil
}

func (m *Manager) CreatePost(postContent utils.PostContent) {
	post := postContent.Post

	// Add post to corresponding timelines
	authorStatistics := m.usersCache.GetUserStatistics(post.AuthorDid)
	go func() {
		for timelineName, algorithm := range m.algorithms {
			if ok, reason := algorithm.AcceptsPost(postContent, authorStatistics); ok {
				m.AddPostToTimeline(timelineName, post, reason)
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
	if err := queries.CreatePost(m.dbSession, post); err != nil {
		log.Errorf("Error creating post: %v", err)
	}

	// Add post to user statistics
	if err := queries.ApplyPostToUser(m.dbSession, post, 1); err != nil {
		log.Errorf("Error increasing user's post count: %v", err)
	}

	// Update cache (exclude replies)
	if post.ReplyRoot == "" {
		m.usersCache.UpdateUserStatistics(
			post.AuthorDid, 0, 0, 1, 0,
		)
	}
}

func (m *Manager) CreateUser(did string) {
	if _, ok := m.usersCreated.Load(did); ok {
		return
	}
	err := queries.CreateUser(
		m.dbSession,
		models.UsersStruct{Did: did},
	)
	if err != nil {
		log.Errorf("Error creating user: %m", err)
		return
	}
	m.usersCreated.Store(did, true)
}

func (m *Manager) DeleteFollow(uri string) {
	// Get follow
	follow, err := queries.GetFollow(m.dbSession, uri)
	if err != nil {
		// Follow does not exist in DB
		return
	}

	// Delete in DB
	if err := queries.DeleteFollow(m.dbSession, uri); err != nil {
		log.Errorf("Error deleting follow: %v", err)
		return
	}

	// Discount follow from users statistics
	if err := queries.ApplyFollowToUsers(m.dbSession, follow, 1); err != nil {
		log.Errorf("Error adding follow to users: %v", err)
		return
	}
	m.usersCache.UpdateUserStatistics(
		follow.AuthorDid, -1, 0, 0, 0,
	)
	m.usersCache.UpdateUserStatistics(
		follow.SubjectDid, 0, -1, 0, 0,
	)
}

func (m *Manager) DeleteInteraction(uri string) {
	// Get interaction
	interaction, err := queries.GetInteraction(m.dbSession, uri)
	if err != nil {
		log.Errorf("Error getting interaction to delete: %v", err)
		return
	}

	// Delete in DB
	if err := queries.DeleteInteraction(m.dbSession, uri); err != nil {
		log.Errorf("Error deleting interaction: %v", err)
		return
	}

	// Update caches
	postAuthor, ok := m.postsCache.GetPostAuthor(interaction.PostUri)
	if ok {
		m.postsCache.DeleteInteraction(interaction.PostUri)
		m.usersCache.UpdateUserStatistics(
			postAuthor, 0, 0, 0, -1,
		)
	}
}

func (m *Manager) DeletePost(uri string) {
	// Get post
	post, err := queries.GetPost(m.dbSession, uri)
	if err != nil {
		log.Errorf("Error getting post to delete: %v", err)
		return
	}

	// Delete in DB
	if err := queries.DeletePost(m.dbSession, uri); err != nil {
		log.Errorf("Error deleting post: %v", err)
		return
	}

	// Remove post from user statistics
	if err := queries.ApplyPostToUser(m.dbSession, post, -1); err != nil {
		log.Errorf("Error decreasing user's post count: %v", err)
		return
	}

	// Update caches
	deleted := m.postsCache.DeletePost(uri)
	if deleted {
		postInteractions := m.postsCache.GetPostInteractions(post.AuthorDid)
		m.usersCache.UpdateUserStatistics(
			post.AuthorDid, 0, 0, -1, -postInteractions,
		)
	}
}

func (m *Manager) DeleteUser(did string) {
	// Delete user
	if err := queries.DeleteUser(m.dbSession, did); err != nil {
		log.Errorf("Error deleting user: %v", err)
		return
	}

	// Delete user's posts
	if err := queries.DeletePostsFromUser(m.dbSession, did); err != nil {
		log.Errorf("Error deleting posts for user %s: %v", did, err)
	}

	// Delete user's interactions
	if err := queries.DeleteInteractionsFromUser(m.dbSession, did); err != nil {
		log.Errorf("Error deleting interactions from user %s: %v", did, err)
	}
	// TODO: Update caches

	// Delete user's follows
	if err := queries.DeleteFollowsFromUser(m.dbSession, did); err != nil {
		log.Errorf("Error deleting follows from user %s: %v", did, err)
	}
	// TODO: Update caches

	// Delete user from cache
	m.usersCache.DeleteUser(did)
}

func (m *Manager) GetCursor(service string) int64 {
	state, _ := queries.GetSubscriptionState(m.dbSession, service)
	return state.Cursor // defaults to 0 if not in DB
}

func (m *Manager) GetOutdatedUserDids() []string {
	dids, err := queries.GetUserDidsToRefreshStatistics(m.dbSession)
	if err != nil {
		log.Errorf("Error getting user dids for update: %v", err)
	}
	return dids
}

func (m *Manager) GetTimeline(timelineName string, maxRank float64, limit int64) []cache.TimelineEntry {
	// Attempt to hit cache first
	timeline, ok := m.timelines[timelineName]
	if !ok {
		panic(fmt.Sprintf("Could not find timeline for feed: %s", timelineName))
	}
	entries := timeline.GetPosts(maxRank, limit)

	// Not found. Go to DB
	if int64(len(entries)) < limit {
		algorithm, ok := m.algorithms[timelineName]
		if !ok {
			panic(fmt.Sprintf("Could not find algorithm for feed: %s", timelineName))
		}
		dbPosts := algorithm.GetPosts(m.dbSession, maxRank, limit)
		for _, post := range dbPosts {
			if !slices.ContainsFunc(entries, func(entry cache.TimelineEntry) bool {
				for _, e := range entries {
					if e.Post.Uri == post.Uri {
						return true
					}
				}
				return false
			}) {
				entries = append(entries, cache.TimelineEntry{Post: post})
				// Add to cache
				timeline.AddPost(post, nil)
			}
		}
	}

	return entries
}

func (m *Manager) UpdateCursor(service string, cursor int64) {
	if err := queries.UpdateSubscriptionStateCursor(m.dbSession, service, cursor); err != nil {
		log.Errorf("Error updating cursor: %v", err)
	}
}

func (m *Manager) UpdateUser(updatedUser models.UsersStruct, updatedUserCounters models.UsersCountersStruct) {
	// Update on cache
	m.usersCache.SetUserFollows(
		updatedUserCounters.Did,
		int32(updatedUserCounters.FollowersCount),
		int32(updatedUserCounters.FollowsCount),
	)

	// Update on DB
	updatedUser.LastUpdate = time.Now()
	if err := queries.UpdateUser(m.dbSession, updatedUser); err != nil {
		log.Errorf("Error updating user '%s': %v", updatedUser.Handle, err)
	}
	if err := queries.UpdateUserCounters(m.dbSession, updatedUserCounters); err != nil {
		log.Errorf("Error updating user '%s' counters: %v", updatedUser.Handle, err)
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
	dids, err := queries.GetUserDids(m.dbSession)
	if err != nil {
		log.Error(err)
		dids = make([]string, 0)
	}

	for _, did := range dids {
		m.usersCreated.Store(did, true)
	}
}
