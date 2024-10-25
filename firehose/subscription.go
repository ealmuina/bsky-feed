package firehose

import (
	"bsky/monitoring/middleware"
	"bsky/storage"
	"bsky/storage/models"
	"bsky/utils"
	"context"
	"encoding/json"
	"fmt"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	jsclient "github.com/bluesky-social/jetstream/pkg/client"
	jsscheduler "github.com/bluesky-social/jetstream/pkg/client/schedulers/sequential"
	jsmodels "github.com/bluesky-social/jetstream/pkg/models"
	log "github.com/sirupsen/logrus"
	"hash"
	"hash/fnv"
	"log/slog"
	"math"
	"net/url"
)

type Subscription struct {
	serviceName       string
	url               url.URL
	languageDetector  *utils.LanguageDetector
	hasher            hash.Hash32
	storageManager    *storage.Manager
	metricsMiddleware *middleware.FirehoseMiddleware
}

func NewSubscription(
	serviceName string,
	url url.URL,
	storageManager *storage.Manager,
) *Subscription {
	s := &Subscription{
		serviceName:      serviceName,
		url:              url,
		languageDetector: utils.NewLanguageDetector(),
		hasher:           fnv.New32a(),
		storageManager:   storageManager,
	}
	s.metricsMiddleware = middleware.NewFirehoseMiddleware(s.processOperation)
	return s
}

func (s *Subscription) Run() {
	client, err := jsclient.NewClient(
		&jsclient.ClientConfig{
			Compress:          false,
			WebsocketURL:      s.url.String(),
			WantedDids:        []string{},
			WantedCollections: []string{},
		},
		slog.Default(),
		jsscheduler.NewScheduler("data_stream", slog.Default(), s.getHandle()),
	)
	if err != nil {
		log.Errorf("Error creating Jetstream client: %v", err)
	}

	cursor := s.storageManager.GetCursor(s.serviceName)
	cursorPointer := &cursor
	if cursor == 0 {
		cursorPointer = nil
	}
	if err := client.ConnectAndRead(context.Background(), cursorPointer); err != nil {
		log.Errorf("Error connecting to Jetstream client: %v", err)
	}

	log.Info("Started consuming from Jetstream")
}

func (s *Subscription) calculateUri(evt *jsmodels.Event) string {
	return fmt.Sprintf("at://%s/%s/%s", evt.Did, evt.Commit.Collection, evt.Commit.RKey)
}

func (s *Subscription) getHandle() func(context.Context, *jsmodels.Event) error {
	var seq uint64 = 0
	return func(ctx context.Context, evt *jsmodels.Event) error {
		if evt.Kind != jsmodels.EventKindCommit {
			return nil
		}
		cursor := evt.TimeUS
		seq++
		if seq%100 == 0 {
			go s.storageManager.UpdateCursor(s.serviceName, cursor)
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

	createdAt, err := utils.ParseTime(post.CreatedAt)
	if err != nil {
		log.Errorf("Error parsing created at: %s", err)
		return err
	}

	replyParent, replyRoot := "", ""
	if post.Reply != nil {
		if post.Reply.Parent != nil {
			replyParent = post.Reply.Parent.Uri
		}
		if post.Reply.Root != nil {
			replyRoot = post.Reply.Root.Uri
		}
	}

	language := s.languageDetector.DetectLanguage(post.Text, post.Langs)

	// Calculate rank
	s.hasher.Write([]byte(uri))
	hash := s.hasher.Sum32()
	s.hasher.Reset()
	decimalPlaces := int(math.Log10(float64(hash))) + 1
	divisor := math.Pow10(decimalPlaces)
	rank := float64(createdAt.Unix()) + float64(hash)/divisor

	go func() {
		s.storageManager.CreateUser(evt.Did)
		s.storageManager.CreatePost(
			models.Post{
				Uri:         uri,
				AuthorDid:   evt.Did,
				ReplyParent: replyParent,
				ReplyRoot:   replyRoot,
				CreatedAt:   createdAt,
				Language:    language,
				Rank:        rank,
				Text:        post.Text,
			Embed:       data.Embed,
	})

	}()

	return nil
}

func (s *Subscription) handleGraphFollowCreate(evt *jsmodels.Event) error {
	var follow appbsky.GraphFollow
	if err := json.Unmarshal(evt.Commit.Record, &follow); err != nil {
		return fmt.Errorf("failed to unmarshal follow: %w", err)
	}

	createdAt, err := utils.ParseTime(follow.CreatedAt)
	if err != nil {
		log.Errorf("Error parsing created at: %s", err)
		return err
	}

	go s.storageManager.CreateFollow(
		models.Follow{
			Uri:        s.calculateUri(evt),
			AuthorDid:  evt.Did,
			SubjectDid: follow.Subject,
			CreatedAt:  createdAt,
		},
	)

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

	createdAt, err := utils.ParseTime(createdAtStr)
	if err != nil {
		log.Errorf("Error parsing created at: %s", err)
		return err
	}

	go func() {
		s.storageManager.CreateUser(evt.Did)
		s.storageManager.CreateInteraction(
			models.Interaction{
				Uri:       s.calculateUri(evt),
				Kind:      kind,
				AuthorDid: evt.Did,
				PostUri:   postUri,
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
	uri := s.calculateUri(evt)

	switch evt.Commit.Collection {
	case "app.bsky.feeds.post":
		s.storageManager.DeletePost(uri)
	case "app.bsky.feeds.like", "app.bsky.feeds.repost":
		s.storageManager.DeleteInteraction(uri)
	case "app.bsky.graph.follow":
		s.storageManager.DeleteFollow(uri)
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
