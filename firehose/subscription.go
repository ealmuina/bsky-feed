package firehose

import (
	"bsky/monitoring/middleware"
	"bsky/storage"
	"bsky/storage/models"
	"bsky/utils"
	"context"
	"encoding/json"
	"fmt"
	"github.com/araddon/dateparse"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	jsclient "github.com/bluesky-social/jetstream/pkg/client"
	jsscheduler "github.com/bluesky-social/jetstream/pkg/client/schedulers/sequential"
	jsmodels "github.com/bluesky-social/jetstream/pkg/models"
	log "github.com/sirupsen/logrus"
	"hash"
	"hash/fnv"
	"log/slog"
	"math"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Subscription struct {
	serviceName       string
	hosts             []string
	languageDetector  *utils.LanguageDetector
	hasher            hash.Hash32
	storageManager    *storage.Manager
	metricsMiddleware *middleware.FirehoseMiddleware
	workerPool        chan struct{}
	wg                sync.WaitGroup
}

func NewSubscription(
	serviceName string,
	hosts []string,
	storageManager *storage.Manager,
) *Subscription {
	// Get worker pool size from environment or use default
	workerPoolSize := utils.IntFromString(os.Getenv("FIREHOSE_WORKER_POOL_SIZE"), 100)

	s := &Subscription{
		serviceName:      serviceName,
		hosts:            hosts,
		languageDetector: utils.NewLanguageDetector(),
		hasher:           fnv.New32a(),
		storageManager:   storageManager,
		workerPool:       make(chan struct{}, workerPoolSize),
	}
	s.metricsMiddleware = middleware.NewFirehoseMiddleware(s.processOperation)
	return s
}

func (s *Subscription) Run() {
	// Create a context that can be canceled
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		// Cancel the context and wait for all workers to finish
		cancel()
		s.wg.Wait()
		log.Info("All workers have completed, subscription shut down cleanly")
	}()

	for {
		client := s.createClient()

		cursorStr := s.storageManager.GetCursor(s.serviceName)
		cursor := int64(utils.IntFromString(cursorStr, 0))

		if cursor > 0 {
			// Subtract one minute to fill any event gap
			cursor = cursor - 1e7 // 10 seconds in microseconds
		}
		cursorPointer := &cursor
		if cursor == 0 {
			cursorPointer = nil
		}

		err := client.ConnectAndRead(ctx, cursorPointer)
		if err != nil {
			if ctx.Err() != nil {
				// Context was canceled, exit the loop
				log.Info("Subscription context canceled, shutting down")
				return
			}
			log.Errorf("Error connecting to Jetstream client: %v", err)
		} else {
			break
		}
	}

	log.Info("Started consuming from Jetstream")
}

func (s *Subscription) calculateUri(evt *jsmodels.Event) string {
	return fmt.Sprintf("at://%s/%s/%s", evt.Did, evt.Commit.Collection, evt.Commit.RKey)
}

func (s *Subscription) createClient() *jsclient.Client {
	host := s.hosts[rand.Intn(len(s.hosts))]
	url := url.URL{
		Scheme: "wss",
		Host:   host,
		Path:   "/subscribe",
	}

	client, err := jsclient.NewClient(
		&jsclient.ClientConfig{
			Compress:          false,
			WebsocketURL:      url.String(),
			WantedDids:        []string{},
			WantedCollections: []string{},
		},
		slog.Default(),
		jsscheduler.NewScheduler("data_stream", slog.Default(), s.getHandle()),
	)
	if err != nil {
		log.Fatalf("Error creating Jetstream client: %v", err)
	}
	return client
}

func (s *Subscription) getHandle() func(context.Context, *jsmodels.Event) error {
	var seq uint64 = 0
	cursor := 0
	return func(ctx context.Context, evt *jsmodels.Event) error {
		if evt.Kind != jsmodels.EventKindCommit {
			return nil
		}
		cursor = int(math.Max(float64(evt.TimeUS), float64(cursor)))
		seq++
		if seq%100 == 0 {
			// Acquire a worker from the pool
			select {
			case s.workerPool <- struct{}{}:
				s.wg.Add(1)
				go func(currentCursor int) {
					defer func() {
						// Release the worker back to the pool
						<-s.workerPool
						s.wg.Done()
					}()
					s.storageManager.UpdateCursor(s.serviceName, strconv.Itoa(currentCursor))
				}(cursor)
			default:
				// If the worker pool is full, update the cursor synchronously
				s.storageManager.UpdateCursor(s.serviceName, strconv.Itoa(cursor))
			}
		}
		if err := s.metricsMiddleware.HandleOperation(evt); err != nil {
			log.Errorf("Error handling operation: %v", err)
		}
		return nil
	}
}

func (s *Subscription) handleFeedPostCreate(evt *jsmodels.Event) error {
	var post appbsky.FeedPost
	if err := json.Unmarshal(evt.Commit.Record, &post); err != nil {
		return fmt.Errorf("failed to unmarshal post: %w", err)
	}

	uri := s.calculateUri(evt)

	createdAt, err := dateparse.ParseAny(post.CreatedAt)
	if err != nil {
		log.Errorf("Error parsing created at: %s", err)
		return err
	}
	if now := time.Now(); now.Before(createdAt) {
		// Created at set to the future. Replace by current time
		createdAt = now
	}

	var replyParentId, replyRootId int64
	if post.Reply != nil {
		if post.Reply.Parent != nil {
			authorDid, uriKey, err := utils.SplitUri(post.Reply.Parent.Uri, "/app.bsky.feed.post/")
			if err != nil {
				return err
			}
			authorId, err := s.storageManager.GetOrCreateUser(authorDid)
			if err != nil {
				return err
			}
			replyParentId, err = s.storageManager.GetPostId(authorId, uriKey)
			if err != nil {
				return err
			}
		}
		if post.Reply.Root != nil {
			authorDid, uriKey, err := utils.SplitUri(post.Reply.Root.Uri, "/app.bsky.feed.post/")
			if err != nil {
				return err
			}
			authorId, err := s.storageManager.GetOrCreateUser(authorDid)
			if err != nil {
				return err
			}
			replyRootId, err = s.storageManager.GetPostId(authorId, uriKey)
			if err != nil {
				return err
			}
		}
	}

	// Acquire a worker from the pool
	s.workerPool <- struct{}{}
	s.wg.Add(1)

	go func() {
		defer func() {
			// Release the worker back to the pool
			<-s.workerPool
			s.wg.Done()
		}()

		language := s.languageDetector.DetectLanguage(post.Text, post.Langs)

		// Calculate rank
		s.hasher.Write([]byte(uri))
		hash := s.hasher.Sum32()
		s.hasher.Reset()
		decimalPlaces := int(math.Log10(float64(hash))) + 1
		divisor := math.Pow10(decimalPlaces)
		rank := float64(createdAt.Unix()) + float64(hash)/divisor

		authorId, err := s.storageManager.GetOrCreateUser(evt.Did)
		if err != nil {
			log.Errorf("Error creating user: %v", err)
			return
		}
		s.storageManager.CreatePost(
			models.Post{
				UriKey:        evt.Commit.RKey,
				AuthorId:      authorId,
				AuthorDid:     evt.Did,
				ReplyParentId: replyParentId,
				ReplyRootId:   replyRootId,
				CreatedAt:     createdAt,
				Language:      language,
				Rank:          rank,
				Text:          post.Text,
				Embed:         post.Embed,
			})
	}()

	return nil
}

func (s *Subscription) handleGraphFollowCreate(evt *jsmodels.Event) error {
	var follow appbsky.GraphFollow
	if err := json.Unmarshal(evt.Commit.Record, &follow); err != nil {
		return fmt.Errorf("failed to unmarshal follow: %w", err)
	}

	createdAt, err := dateparse.ParseAny(follow.CreatedAt)
	if err != nil {
		log.Errorf("Error parsing created at: %s", err)
		return err
	}

	// Acquire a worker from the pool
	s.workerPool <- struct{}{}
	s.wg.Add(1)

	go func() {
		defer func() {
			// Release the worker back to the pool
			<-s.workerPool
			s.wg.Done()
		}()

		authorId, err := s.storageManager.GetOrCreateUser(evt.Did)
		if err != nil {
			log.Errorf("Error creating user: %v", err)
			return
		}
		subjectId, err := s.storageManager.GetOrCreateUser(follow.Subject)
		if err != nil {
			log.Errorf("Error creating user: %v", err)
			return
		}
		s.storageManager.CreateFollow(
			models.Follow{
				UriKey:    evt.Commit.RKey,
				AuthorID:  authorId,
				SubjectID: subjectId,
				CreatedAt: createdAt,
			},
		)
	}()

	return nil
}

func (s *Subscription) handleInteractionCreate(evt *jsmodels.Event) error {
	var createdAtStr string
	var postUri string
	var kind models.InteractionType

	switch evt.Commit.Collection {
	case "app.bsky.feed.like":
		var like appbsky.FeedLike
		if err := json.Unmarshal(evt.Commit.Record, &like); err != nil {
			return fmt.Errorf("failed to unmarshal like: %w", err)
		}
		createdAtStr = like.CreatedAt
		postUri = like.Subject.Uri
		kind = models.Like

	case "app.bsky.feed.repost":
		var repost appbsky.FeedRepost
		if err := json.Unmarshal(evt.Commit.Record, &repost); err != nil {
			return fmt.Errorf("failed to unmarshal repost: %w", err)
		}
		createdAtStr = repost.CreatedAt
		postUri = repost.Subject.Uri
		kind = models.Repost
	}

	if !strings.Contains(postUri, "/app.bsky.feed.post/") {
		// Likes can be given to feeds too
		return nil
	}

	createdAt, err := dateparse.ParseAny(createdAtStr)
	if err != nil {
		log.Errorf("Error parsing created at: %s", err)
		return err
	}

	// Acquire a worker from the pool
	s.workerPool <- struct{}{}
	s.wg.Add(1)

	go func() {
		defer func() {
			// Release the worker back to the pool
			<-s.workerPool
			s.wg.Done()
		}()

		authorId, err := s.storageManager.GetOrCreateUser(evt.Did)
		if err != nil {
			log.Errorf("Error creating user: %v", err)
			return
		}
		postAuthorDid, postUriKey, err := utils.SplitUri(postUri, "/app.bsky.feed.post/")
		if err != nil {
			log.Errorf("Error parsing post uri: %v", err)
			return
		}
		postAuthorId, err := s.storageManager.GetOrCreateUser(postAuthorDid)
		if err != nil {
			log.Errorf("Error creating user: %v", err)
			return
		}
		postId, err := s.storageManager.GetPostId(postAuthorId, postUriKey)
		if err != nil {
			log.Errorf("Error getting post id: %v", err)
			return
		}
		s.storageManager.CreateInteraction(
			models.Interaction{
				UriKey:    evt.Commit.RKey,
				Kind:      kind,
				AuthorId:  authorId,
				PostId:    postId,
				CreatedAt: createdAt,
			},
		)
	}()

	return nil
}

func (s *Subscription) handleRecordCreate(evt *jsmodels.Event) error {
	var handleFunc func(evt *jsmodels.Event) error

	switch evt.Commit.Collection {
	case "app.bsky.feed.post":
		handleFunc = s.handleFeedPostCreate
	case "app.bsky.feed.like", "app.bsky.feed.repost":
		handleFunc = s.handleInteractionCreate
	case "app.bsky.graph.follow":
		handleFunc = s.handleGraphFollowCreate
	}

	if handleFunc == nil {
		return nil
	}
	return handleFunc(evt)
}

func (s *Subscription) handleRecordDelete(evt *jsmodels.Event) error {
	authorId, err := s.storageManager.GetOrCreateUser(evt.Did)
	if err != nil {
		log.Errorf("Error creating user: %v", err)
		return err
	}
	identifier := models.Identifier{
		UriKey:   evt.Commit.RKey,
		AuthorId: authorId,
	}

	switch evt.Commit.Collection {
	case "app.bsky.feeds.post":
		s.storageManager.DeletePost(identifier)
	case "app.bsky.feeds.like", "app.bsky.feeds.repost":
		s.storageManager.DeleteInteraction(identifier)
	case "app.bsky.graph.follow":
		s.storageManager.DeleteFollow(identifier)
	}

	return nil
}

func (s *Subscription) processOperation(evt *jsmodels.Event) error {
	switch evt.Commit.Operation {
	case jsmodels.CommitOperationCreate:
		if err := s.handleRecordCreate(evt); err != nil {
			log.Errorf("Error handling create record: %s", err)
			return err
		}
	case jsmodels.CommitOperationDelete:
		if err := s.handleRecordDelete(evt); err != nil {
			log.Errorf("Error handling delete record: %s", err)
			return err
		}
	}

	return nil
}
