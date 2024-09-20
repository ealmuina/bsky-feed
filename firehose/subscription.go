package firehose

import (
	"bsky/storage"
	db "bsky/storage/db/sqlc"
	"bsky/storage/models"
	"bsky/utils"
	"bytes"
	"context"
	"fmt"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	typegen "github.com/whyrusleeping/cbor-gen"
	"hash"
	"hash/fnv"
	"math"
	"net/url"
	"strings"
)

const SchedulerMaxConcurrency = 8

type Subscription struct {
	serviceName      string
	url              url.URL
	connection       *websocket.Conn
	languageDetector *utils.LanguageDetector
	hasher           hash.Hash32
	storageManager   *storage.Manager
}

func getConnection(url url.URL) *websocket.Conn {
	c, _, err := websocket.DefaultDialer.Dial(url.String(), nil)
	if err != nil {
		log.Error(err)
	}
	return c
}

func NewSubscription(
	serviceName string,
	url url.URL,
	storageManager *storage.Manager,
) *Subscription {
	if cursor := storageManager.GetCursor(serviceName); cursor != 0 {
		url.RawQuery = fmt.Sprintf("cursor=%v", cursor)
	}

	return &Subscription{
		serviceName:      serviceName,
		url:              url,
		connection:       getConnection(url),
		languageDetector: utils.NewLanguageDetector(),
		hasher:           fnv.New32a(),
		storageManager:   storageManager,
	}
}

func (s *Subscription) Run() {
	defer s.close()

	scheduler := parallel.NewScheduler(
		SchedulerMaxConcurrency,
		1000,
		"data_stream",
		s.getHandle(),
	)

	for {
		err := events.HandleRepoStream(
			context.Background(),
			s.connection,
			scheduler,
		)
		if err != nil {
			log.Panic(err)
		}
		if recover() != nil {
			// Reset connection
			s.connection = getConnection(s.url)
		}
	}
}

func (s *Subscription) close() {
	err := s.connection.Close()
	if err != nil {
		log.Error(err)
	}
}

func (s *Subscription) getHandle() func(context.Context, *events.XRPCStreamEvent) error {
	return func(ctx context.Context, evt *events.XRPCStreamEvent) error {
		if commit := evt.RepoCommit; commit != nil {
			if commit.Seq%100 == 0 {
				go s.storageManager.UpdateCursor(s.serviceName, commit.Seq)
			}

			rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(commit.Blocks))
			if err != nil {
				log.Errorf("Error getting repo from car: %s", err)
				return err
			}

			for _, op := range commit.Ops {
				uri := fmt.Sprintf("at://%s/%s", commit.Repo, op.Path)

				switch repomgr.EventKind(op.Action) {
				case repomgr.EvtKindCreateRecord:
					_, record, err := rr.GetRecord(ctx, op.Path)
					if err != nil {
						log.Errorf("Error getting record: %s", err)
						return err
					}

					if err := s.handleRecordCreate(
						commit.Repo, uri, op.Cid, record,
					); err != nil {
						log.Errorf("Error handling create record: %s", err)
						return err
					}
				case repomgr.EvtKindDeleteRecord:
					recordType := strings.Split(op.Path, "/")[0]
					if err := s.handleRecordDelete(uri, recordType); err != nil {
						log.Errorf("Error handling delete record: %s", err)
						return err
					}
				}
			}
		}
		return nil
	}
}

func (s *Subscription) handleFeedPostCreate(
	repoDID string,
	uri string,
	data *appbsky.FeedPost,
) error {
	createdAt, err := utils.ParseTime(data.CreatedAt)
	if err != nil {
		log.Errorf("Error parsing created at: %s", err)
		return err
	}

	replyParent, replyRoot := "", ""
	if data.Reply != nil {
		if data.Reply.Parent != nil {
			replyParent = data.Reply.Parent.Uri
		}
		if data.Reply.Root != nil {
			replyRoot = data.Reply.Root.Uri
		}
	}

	language := s.languageDetector.DetectLanguage(data.Text, data.Langs)

	// Calculate rank
	s.hasher.Write([]byte(uri))
	hash := s.hasher.Sum32()
	s.hasher.Reset()
	decimalPlaces := int(math.Log10(float64(hash))) + 1
	divisor := math.Pow10(decimalPlaces)
	rank := float64(createdAt.Unix()) + float64(hash)/divisor

	post := models.Post{
		Uri:         uri,
		AuthorDid:   repoDID,
		ReplyParent: replyParent,
		ReplyRoot:   replyRoot,
		CreatedAt:   createdAt,
		Language:    language,
		Rank:        rank,
	}

	go func() {
		s.storageManager.CreateUser(repoDID)
		s.storageManager.CreatePost(post)
	}()

	return nil
}

func (s *Subscription) handleGraphFollowCreate(
	repoDID string,
	uri string,
	data *appbsky.GraphFollow,
) error {
	createdAt, err := utils.ParseTime(data.CreatedAt)
	if err != nil {
		log.Errorf("Error parsing created at: %s", err)
		return err
	}

	go s.storageManager.CreateFollow(
		models.Follow{
			Uri:        uri,
			AuthorDid:  repoDID,
			SubjectDid: data.Subject,
			CreatedAt:  createdAt,
		},
	)

	return nil
}

func (s *Subscription) handleInteractionCreate(
	repoDID string,
	uri string,
	cid *util.LexLink,
	kind db.InteractionType,
	createdAtStr string,
	postUri string,
) error {
	createdAt, err := utils.ParseTime(createdAtStr)
	if err != nil {
		log.Errorf("Error parsing created at: %s", err)
		return err
	}

	go func() {
		s.storageManager.CreateUser(
			repoDID,
		)
		s.storageManager.CreateInteraction(
			repoDID, uri, cid, kind, createdAt, postUri,
		)
	}()

	return nil
}

func (s *Subscription) handleRecordCreate(
	repoDID string,
	uri string,
	cid *util.LexLink,
	record typegen.CBORMarshaler,
) error {
	var err error = nil
	switch data := record.(type) {
	case *appbsky.FeedPost:
		err = s.handleFeedPostCreate(
			repoDID, uri, data,
		)
	case *appbsky.FeedLike:
		err = s.handleInteractionCreate(
			repoDID,
			uri,
			cid,
			db.InteractionTypeLike,
			data.CreatedAt,
			data.Subject.Uri,
		)
	case *appbsky.FeedRepost:
		err = s.handleInteractionCreate(
			repoDID,
			uri,
			cid,
			db.InteractionTypeRepost,
			data.CreatedAt,
			data.Subject.Uri,
		)
	case *appbsky.GraphFollow:
		err = s.handleGraphFollowCreate(
			repoDID, uri, data,
		)
	}
	return err
}

func (s *Subscription) handleRecordDelete(uri string, recordType string) error {
	switch recordType {
	case "app.bsky.feeds.post":
		s.storageManager.DeletePost(uri)
	case "app.bsky.feeds.like", "app.bsky.feeds.repost":
		s.storageManager.DeleteInteraction(uri)
	case "app.bsky.graph.follow":
		s.storageManager.DeleteFollow(uri)
	}
	return nil
}
