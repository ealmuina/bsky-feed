package storage

import (
	"bsky/storage/algorithms"
	"bsky/storage/cache"
	"bsky/storage/models"
	"bsky/utils"
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"os"
	"strings"
	"time"
)

type Manager struct {
	redisConnection *redis.Client
	dbConnection    *mongo.Database
	persistFollows  bool

	usersCache cache.UsersCache
	postsCache cache.PostsCache
	timelines  map[string]cache.Timeline
	algorithms map[string]algorithms.Algorithm
}

func NewManager(dbConnection *mongo.Database, redisConnection *redis.Client, persistFollows bool) *Manager {
	usersCacheExpiration := utils.IntFromString(
		os.Getenv("USERS_CACHE_EXPIRATION_MINUTES"), 43200,
	)
	postsCacheExpiration := utils.IntFromString(
		os.Getenv("POSTS_CACHE_EXPIRATION_MINUTES"), 1080,
	)

	storageManager := Manager{
		redisConnection: redisConnection,
		dbConnection:    dbConnection,
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
	//// Clean DB
	//ctx := context.Background()
	//if !persistentDb {
	//	if err := m.queries.DeleteOldInteractions(ctx); err != nil {
	//		log.Errorf("Error cleaning old interactions: %v", err)
	//	}
	//}
	//
	//// Clean timelines
	//for _, timeline := range m.timelines {
	//	timeline.DeleteExpiredPosts(time.Now().Add(-3 * 24 * time.Hour)) // Timelines lifespan of 3 days
	//}
	//
	//// Clean caches
	//oldPosts, err := m.queries.GetOldPosts(ctx)
	//if err != nil {
	//	log.Errorf("Error retrieving old posts: %v", err)
	//}
	//postIds := make([]int64, 0, len(oldPosts))
	//for _, post := range oldPosts {
	//	postIds = append(postIds, post.ID)
	//
	//	// Discount from user statistics
	//	postInteractions := m.postsCache.GetPostInteractions(post.ID)
	//	m.usersCache.UpdateUserStatistics(post.AuthorID, 0, 0, -1, -postInteractions)
	//}
	//// Delete from posts cache
	//m.postsCache.DeletePosts(postIds)
}

func (m *Manager) CreateFollow(follow models.Follow) {
	usersColl := m.dbConnection.Collection("users")
	followsColl := m.dbConnection.Collection("follows")

	if m.persistFollows {
		err := m.executeTransaction(
			func(ctx mongo.SessionContext) (interface{}, error) {
				// Insert follow
				filter := bson.M{"author_id": follow.AuthorID, "subject_id": follow.SubjectID}
				update := bson.M{
					"$set": bson.M{
						"uri_key": bson.M{
							"$cond": bson.M{
								"if":   bson.M{"$gt": bson.A{"$created_at", follow.CreatedAt}},
								"then": "$uri_key",
								"else": follow.UriKey,
							},
						},
						"created_at": bson.M{
							"$max": bson.A{"$created_at", follow.CreatedAt},
						},
					},
				}
				opts := options.Update().SetUpsert(true)
				result, err := followsColl.UpdateOne(ctx, filter, update, opts)
				if err != nil {
					log.Errorf("Error creating follow '%s/%s': %v", follow.AuthorID, follow.UriKey, err)
					return nil, err
				}

				if result.UpsertedCount > 0 { // created
					// Increase follows count
					authorId, _ := primitive.ObjectIDFromHex(follow.AuthorID)
					_, err = usersColl.UpdateOne(
						ctx,
						bson.D{{"_id", authorId}},
						bson.D{{"$inc", bson.D{{"follows_count", 1}}}},
					)
					// Increase followers count
					subjectId, _ := primitive.ObjectIDFromHex(follow.SubjectID)
					_, err = usersColl.UpdateOne(
						ctx,
						bson.D{{"_id", subjectId}},
						bson.D{{"$inc", bson.D{{"followers_count", 1}}}},
					)
				}
				return result, err
			},
		)

		// Update caches
		if err == nil {
			m.refreshFollowStatistics(follow.AuthorID, follow.SubjectID, 1, false)
		}
	} else {
		// Do not persist. Just update statistics
		m.refreshFollowStatistics(follow.AuthorID, follow.SubjectID, 1, true)
	}
}

func (m *Manager) CreateInteraction(interaction models.Interaction) {
	postsColl := m.dbConnection.Collection("posts")
	interactionsColl := m.dbConnection.Collection("interactions")

	err := m.executeTransaction(
		func(ctx mongo.SessionContext) (interface{}, error) {
			// Insert interactions
			filter := bson.M{
				"author_id": interaction.AuthorId,
				"post_id":   interaction.PostId,
				"kind":      interaction.Kind,
			}
			update := bson.M{
				"$setOnInsert": bson.M{
					"author_id": interaction.AuthorId,
					"kind":      interaction.Kind,
					"post_id":   interaction.PostId,
				}, // Insert these values if no matching document exists
				"$set": bson.M{
					"uri_key": bson.M{
						"$cond": bson.M{
							"if":   bson.M{"$gt": bson.A{"$created_at", interaction.CreatedAt}},
							"then": "$uri_key",         // Keep existing uri_key
							"else": interaction.UriKey, // Update to new uri_key
						},
					},
					"created_at": bson.M{
						"$max": bson.A{"$created_at", interaction.CreatedAt}, // Update created_at to GREATEST(existing, new)
					},
				},
			}
			opts := options.Update().SetUpsert(true)

			result, err := interactionsColl.UpdateOne(ctx, filter, update, opts)
			if err != nil {
				log.Errorf(
					"Error creating interaction '%s/%s': %v",
					interaction.AuthorId,
					interaction.UriKey,
					err,
				)
				return nil, err
			}

			if result.UpsertedCount > 0 { // created
				// Increase counter
				postId, _ := primitive.ObjectIDFromHex(interaction.PostId)
				counterField := "likes_count"
				if interaction.Kind != models.Like {
					counterField = "follows_count"
				}
				_, err = postsColl.UpdateOne(
					ctx,
					bson.D{{"_id", postId}},
					bson.D{{"$inc", bson.D{{counterField, 1}}}},
				)
			}
			return result, err
		},
	)

	// Update caches
	if err == nil {
		m.postsCache.AddInteraction(interaction.PostId)
		if postAuthorId, err := m.GetPostAuthorId(interaction.PostId); err == nil { // post author found
			m.usersCache.UpdateUserStatistics(
				postAuthorId, 0, 0, 0, 1,
			)
		}
	}
}

func (m *Manager) CreatePost(post models.Post) {
	usersColl := m.dbConnection.Collection("users")
	postsColl := m.dbConnection.Collection("posts")

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

	var result *mongo.UpdateResult
	var err error
	err = m.executeTransaction(func(ctx mongo.SessionContext) (interface{}, error) {
		// Upsert post
		result, err = postsColl.ReplaceOne(
			ctx,
			bson.D{{"uri_key", post.UriKey}, {"author_id", post.AuthorId}},
			post,
			options.Replace().SetUpsert(true),
		)
		if err != nil {
			if !strings.Contains(err.Error(), "Please retry your operation or multi-document transaction") {
				log.Errorf("Error upserting post '%s/%s': %v", post.AuthorId, post.UriKey, err)
			}
			return nil, err
		}

		// Increase author's post count if created
		if result.ModifiedCount == 0 {
			post.Id = result.UpsertedID.(primitive.ObjectID)
			authorId, _ := primitive.ObjectIDFromHex(post.AuthorId)
			result, err = usersColl.UpdateOne(
				ctx,
				bson.D{{"_id", authorId}},
				bson.D{{"$inc", bson.D{{"posts_count", 1}}}},
			)
		}

		return result, nil
	})
	if err != nil {
		return
	}

	// Update caches on creation
	if result.ModifiedCount == 0 {
		m.postsCache.AddPost(post)

		// Exclude replies in users cache
		if post.ReplyRootId == "" {
			m.usersCache.UpdateUserStatistics(
				post.AuthorId, 0, 0, 1, 0,
			)
		}
	}
}

func (m *Manager) DeleteFollow(identifier models.Identifier) {
	ctx := context.Background()
	usersColl := m.dbConnection.Collection("users")
	followsColl := m.dbConnection.Collection("follows")

	if m.persistFollows {
		var follow models.Follow
		err := m.executeTransaction(func(ctx mongo.SessionContext) (interface{}, error) {
			// Delete follow
			err := followsColl.FindOneAndDelete(
				ctx,
				bson.D{{"uri_key", identifier.UriKey}, {"author_id", identifier.AuthorId}},
			).Decode(
				&follow,
			)
			if err != nil {
				// ErrNoDocuments means that the filter did not match any documents in  the collection.
				if errors.Is(err, mongo.ErrNoDocuments) {
					return nil, nil
				}
				log.Errorf("Error deleting follow: %v", err)
				return nil, err
			}

			// Decrease follows count
			authorId, _ := primitive.ObjectIDFromHex(follow.AuthorID)
			result, err := usersColl.UpdateOne(
				ctx,
				bson.D{{"_id", authorId}},
				bson.D{{"$inc", bson.D{{"follows_count", -1}}}},
			)
			// Decrease followers count
			subjectId, _ := primitive.ObjectIDFromHex(follow.SubjectID)
			result, err = usersColl.UpdateOne(
				ctx,
				bson.D{{"_id", subjectId}},
				bson.D{{"$inc", bson.D{{"followers_count", -1}}}},
			)

			return result, nil
		})
		if err != nil {
			return
		}

		// Update caches
		m.refreshFollowStatistics(follow.AuthorID, follow.SubjectID, -1, false)
	} else {
		// Discount from follower statistics directly
		authorId, _ := primitive.ObjectIDFromHex(identifier.AuthorId)
		_, err := usersColl.UpdateOne(
			ctx,
			bson.D{{"_id", authorId}},
			bson.D{{"$inc", bson.D{{"follows_count", -1}}}},
		)
		if err == nil {
			m.usersCache.UpdateUserStatistics(
				identifier.AuthorId, -1, 0, 0, 0,
			)
		}
	}
}

func (m *Manager) DeleteInteraction(identifier models.Identifier) {
	postsColl := m.dbConnection.Collection("posts")
	interactionsColl := m.dbConnection.Collection("interactions")

	var interaction models.Interaction
	err := m.executeTransaction(func(ctx mongo.SessionContext) (interface{}, error) {
		// Delete interaction
		err := interactionsColl.FindOneAndDelete(
			ctx,
			bson.D{{"uri_key", identifier.UriKey}, {"author_id", identifier.AuthorId}},
		).Decode(&interaction)
		if err != nil {
			log.Errorf("Error deleting interaction: %v", err)
			return nil, err
		}

		// Decrease counter
		postId, _ := primitive.ObjectIDFromHex(interaction.PostId)
		counterField := "likes_count"
		if interaction.Kind != models.Like {
			counterField = "follows_count"
		}
		result, err := postsColl.UpdateOne(
			ctx,
			bson.D{{"_id", postId}},
			bson.D{{"$inc", bson.D{{counterField, -1}}}},
		)

		return result, err
	})
	if err != nil {
		return
	}

	// Update caches
	if m.postsCache.DeleteInteraction(interaction.PostId) {
		if postAuthorId, err := m.GetPostAuthorId(interaction.PostId); err == nil { // post author found
			m.usersCache.UpdateUserStatistics(
				postAuthorId, 0, 0, 0, -1,
			)
		}
	}
}

func (m *Manager) DeletePost(identifier models.Identifier) {
	usersColl := m.dbConnection.Collection("users")
	postsColl := m.dbConnection.Collection("posts")

	var post models.Post
	err := m.executeTransaction(func(ctx mongo.SessionContext) (interface{}, error) {
		// Delete post
		result, err := postsColl.DeleteOne(
			ctx,
			bson.D{{"uri_key", identifier.UriKey}, {"author_id", identifier.AuthorId}},
		)
		if err != nil {
			log.Errorf("Error deleting post: %v", err)
			return nil, err
		}

		// Decrease author's post count
		authorId, _ := primitive.ObjectIDFromHex(post.AuthorId)
		_, err = usersColl.UpdateOne(
			ctx,
			bson.D{{"_id", authorId}},
			bson.D{{"$inc", bson.D{{"posts_count", -1}}}},
		)

		return result, err
	})
	if err != nil {
		return
	}

	// Update caches
	postId := post.Id.Hex()
	m.postsCache.DeletePost(postId)
	postInteractionsCount := m.postsCache.GetPostInteractions(postId)
	if postInteractionsCount > 0 {
		m.usersCache.UpdateUserStatistics(
			identifier.AuthorId, 0, 0, -1, postInteractionsCount,
		)
	}
}

func (m *Manager) DeleteUser(did string) {
	usersColl := m.dbConnection.Collection("users")
	postsColl := m.dbConnection.Collection("posts")
	followsColl := m.dbConnection.Collection("follows")
	interactionsColl := m.dbConnection.Collection("interactions")

	var user models.User
	var userPosts []models.Post

	err := m.executeTransaction(func(ctx mongo.SessionContext) (interface{}, error) {
		// Delete user
		err := usersColl.FindOneAndDelete(
			ctx,
			bson.D{{"did", did}},
		).Decode(&user)
		if err != nil {
			log.Errorf("Error deleting user: %v", err)
			return nil, err
		}

		// Delete follows
		_, err = followsColl.DeleteMany(
			ctx,
			bson.D{
				{
					"$or", bson.A{
						bson.D{{"author_id", user.Id}},
						bson.D{{"subject_id", user.Id}},
					},
				},
			},
		)
		if err != nil {
			log.Errorf("Error deleting user follows: %v", err)
			return nil, err
		}

		// Retrieve user posts
		userPostsCursor, err := postsColl.Find(
			ctx,
			bson.D{{"author_id", user.Id}},
			options.Find().SetProjection(bson.D{{"_id", 1}}),
		)
		if err != nil {
			log.Errorf("Error finding user posts: %v", err)
			return nil, err
		}
		if err = userPostsCursor.All(ctx, &userPosts); err != nil {
			log.Errorf("Error finding user posts: %v", err)
			return nil, err
		}
		postIds := make([]string, len(userPosts))
		for i, post := range userPosts {
			postIds[i] = post.Id.Hex()
		}

		// Delete interactions
		_, err = interactionsColl.DeleteMany(
			ctx,
			bson.D{
				{
					"$or", bson.A{
						bson.D{{"author_id", user.Id}},
						bson.M{"post_id": bson.M{"$in": postIds}},
					},
				},
			},
		)
		if err != nil {
			log.Errorf("Error deleting user follows: %v", err)
			return nil, err
		}

		// Delete posts
		_, err = postsColl.DeleteMany(
			ctx,
			bson.D{{"author_id", user.Id}},
		)
		if err != nil {
			log.Errorf("Error deleting user posts: %v", err)
			return nil, err
		}

		return nil, nil
	})
	if err != nil {
		return
	}

	// Delete user from cache
	id, ok := m.usersCache.UserDidToId(did)
	if ok {
		m.usersCache.DeleteUser(id)
	}
}

func (m *Manager) GetCursor(service string) string {
	ctx := context.Background()
	coll := m.dbConnection.Collection("subscription_state")

	var result bson.M
	err := coll.FindOne(
		ctx,
		bson.D{{"service", service}},
		options.FindOne().SetProjection(bson.D{{"cursor", 1}}),
	).Decode(&result)
	if err != nil {
		return ""
	}
	cursor, _ := result["cursor"].(string)
	return cursor
}

func (m *Manager) GetOrCreateUser(did string) (id string, err error) {
	if id, ok := m.usersCache.UserDidToId(did); ok {
		return id, nil
	}

	ctx := context.Background()
	coll := m.dbConnection.Collection("users")

	var result bson.M
	err = coll.FindOneAndUpdate(
		ctx,
		bson.D{{"did", did}},
		bson.M{
			"$set": bson.M{"did": did},
		},
		options.
			FindOneAndUpdate().
			SetReturnDocument(options.After).
			SetUpsert(true).
			SetProjection(bson.D{{"_id", 1}}),
	).Decode(&result)
	if err != nil {
		log.Infof("Error creating user '%s': %v", did, err)
		return
	}

	id = result["_id"].(primitive.ObjectID).Hex()
	m.usersCache.AddUser(id, did)

	return id, nil
}

func (m *Manager) GetOutdatedUserDids() []string {
	ctx := context.Background()
	coll := m.dbConnection.Collection("users")
	now := time.Now()

	filter := bson.M{
		"$or": []bson.M{
			{"last_update": bson.M{"$eq": nil}}, // last_update IS NULL
			{"last_update": bson.M{
				"$lt": bson.M{
					"$dateSubtract": bson.M{
						"startDate": now,
						"unit":      "day",
						"amount": bson.M{
							"$ifNull": []interface{}{"$refresh_frequency", 30}, // COALESCE(refresh_frequency, 30)
						},
					},
				},
			}},
		},
	}
	cursor, err := coll.Find(
		ctx,
		filter,
		options.Find().SetProjection(bson.D{{"did", 1}}),
	)
	if err != nil {
		log.Errorf("Error finding outdated users: %v", err)
		return nil
	}

	var results []bson.M
	err = cursor.All(ctx, &results)
	if err != nil {
		log.Errorf("Error finding outdated users: %v", err)
		return nil
	}

	dids := make([]string, len(results))
	for i, result := range results {
		dids[i] = result["did"].(string)
	}
	return dids
}

func (m *Manager) GetPdsSubscriptions() []string {
	ctx := context.Background()
	coll := m.dbConnection.Collection("subscription_state")

	cursor, err := coll.Find(
		ctx,
		bson.D{
			{
				"service", bson.D{
					{"$nin", bson.A{"firehose", "backfill"}},
				},
			},
		},
		options.Find().SetProjection(bson.D{{"service", 1}}),
	)
	if err != nil {
		log.Errorf("Error finding subscriptions: %v", err)
		return nil
	}

	var result []bson.M
	err = cursor.All(ctx, &result)
	if err != nil {
		log.Errorf("Error finding subscriptions: %v", err)
		return nil
	}

	pdsSubscriptions := make([]string, len(result))
	for i, result := range result {
		pdsSubscriptions[i] = result["service"].(string)
	}
	return pdsSubscriptions
}

func (m *Manager) GetPostAuthorId(postIdStr string) (string, error) {
	if authorId, ok := m.postsCache.GetPostAuthorId(postIdStr); ok {
		return authorId, nil
	}

	ctx := context.Background()
	coll := m.dbConnection.Collection("posts")

	postId, _ := primitive.ObjectIDFromHex(postIdStr)
	var result bson.M

	err := coll.FindOne(
		ctx,
		bson.D{{"_id", postId}},
		options.FindOne().SetProjection(bson.D{{"author_id", 1}}),
	).Decode(&result)
	if err != nil {
		log.Errorf("Error finding post author id: %v", err)
		return "", err
	}

	return result["author_id"].(string), nil
}

func (m *Manager) GetPostId(authorId string, uriKey string) (string, error) {
	if postId, ok := m.postsCache.GetPostId(authorId, uriKey); ok {
		return postId, nil
	}

	ctx := context.Background()
	coll := m.dbConnection.Collection("posts")

	var result bson.M
	err := coll.FindOneAndUpdate(
		ctx,
		bson.D{{"author_id", authorId}, {"uri_key", uriKey}},
		bson.M{
			"$set": bson.M{"author_id": authorId, "uri_key": uriKey},
		},
		options.
			FindOneAndUpdate().
			SetReturnDocument(options.After).
			SetUpsert(true).
			SetProjection(bson.D{{"_id", 1}}),
	).Decode(&result)
	if err != nil {
		log.Errorf("Error upserting post '%s/%s': %v", authorId, uriKey, err)
		return "", err
	}

	id := result["_id"].(primitive.ObjectID)
	m.postsCache.AddPost(models.Post{
		Id:       id,
		AuthorId: authorId,
		UriKey:   uriKey,
	})

	return id.Hex(), nil
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
	coll := m.dbConnection.Collection("users")

	_, err := coll.UpdateOne(
		ctx,
		bson.D{{"did", did}},
		bson.M{
			"$set": bson.D{
				{"did", did},
				{"handle", handle},
			},
			"$min": bson.D{{"created_at", createdAt}},
		},
	)
	if err != nil {
		log.Errorf("Error setting user metadata: %v", err)
	}
}

func (m *Manager) UpdateCursor(service string, cursor string) {
	ctx := context.Background()
	coll := m.dbConnection.Collection("subscription_state")

	_, err := coll.UpdateOne(
		ctx,
		bson.D{{"service", service}},
		bson.M{
			"$set": bson.M{"service": service, "cursor": cursor},
		},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		log.Errorf("Error updating cursor: %v", err)
	}
}

func (m *Manager) UpdateUser(updatedUser models.User) {
	ctx := context.Background()
	coll := m.dbConnection.Collection("users")

	// Update on cache
	m.usersCache.SetUserFollows(updatedUser.Id.Hex(), updatedUser.FollowersCount, updatedUser.FollowsCount)

	// Update on DB
	_, err := coll.UpdateOne(
		ctx,
		bson.D{{"did", updatedUser.Did}},
		bson.M{
			"$set": updatedUser,
		},
	)
	if err != nil {
		log.Errorf("Error updating user: %v", err)
	}
}

func (m *Manager) executeTransaction(operation func(ctx mongo.SessionContext) (interface{}, error)) error {
	ctx := context.Background()

	// Start transaction
	client := m.dbConnection.Client()
	wc := writeconcern.Majority()
	txnOptions := options.Transaction().SetWriteConcern(wc)

	session, err := client.StartSession()
	if err != nil {
		panic(err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, operation, txnOptions)
	if err != nil {
		if !strings.Contains(err.Error(), "E11000 duplicate key error collection") {
			log.Warningf("Error committing transaction: %v", err)
		}
	}
	return err
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

func (m *Manager) refreshFollowStatistics(authorIdStr, subjectIdStr string, delta int32, writeToDb bool) {
	ctx := context.Background()
	coll := m.dbConnection.Collection("follows")
	var err error

	// Count on follower
	if writeToDb {
		authorId, _ := primitive.ObjectIDFromHex(authorIdStr)
		_, err = coll.UpdateOne(
			ctx,
			bson.D{{"_id", authorId}},
			bson.D{{"$inc", bson.D{{"follows_count", delta}}}},
		)
	}
	if err == nil {
		m.usersCache.UpdateUserStatistics(
			authorIdStr, int64(delta), 0, 0, 0,
		)
	}

	// Count on followed
	if writeToDb {
		subjectId, _ := primitive.ObjectIDFromHex(subjectIdStr)
		_, err = coll.UpdateOne(
			ctx,
			bson.D{{"_id", subjectId}},
			bson.D{{"$inc", bson.D{{"followers_count", delta}}}},
		)
	}
	if err == nil {
		m.usersCache.UpdateUserStatistics(
			subjectIdStr, 0, int64(delta), 0, 0,
		)
	}
}
