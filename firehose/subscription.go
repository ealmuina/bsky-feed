package firehose

import (
	db "bsky/db/sqlc"
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
	"net/url"
	"strings"
)

const SchedulerMaxConcurrency = 8

type Subscription struct {
	Service          string
	url              url.URL
	connection       *websocket.Conn
	languageDetector *utils.LanguageDetector
	storage          *Storage
}

func getConnection(url url.URL) *websocket.Conn {
	c, _, err := websocket.DefaultDialer.Dial(url.String(), nil)
	if err != nil {
		log.Error(err)
	}
	return c
}

func New(service string, queries *db.Queries, url url.URL) *Subscription {
	storage := NewStorage(queries)

	if cursor := storage.GetCursor(service); cursor != 0 {
		url.RawQuery = fmt.Sprintf("cursor=%v", cursor)
	}

	return &Subscription{
		Service:          service,
		url:              url,
		connection:       getConnection(url),
		languageDetector: utils.NewLanguageDetector(),
		storage:          &storage,
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
				go s.storage.UpdateCursor(s.Service, commit.Seq)
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
						ctx, commit.Repo, uri, op.Cid, record,
					); err != nil {
						log.Errorf("Error handling create record: %s", err)
						return err
					}
				case repomgr.EvtKindDeleteRecord:
					recordType := strings.Split(op.Path, "/")[0]
					if err := s.handleRecordDelete(ctx, uri, recordType); err != nil {
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
	ctx context.Context,
	repoDID string,
	uri string,
	cid *util.LexLink,
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

	s.storage.CreateUser(
		repoDID,
	)
	return s.storage.CreatePost(
		ctx, uri, repoDID, cid, replyParent, replyRoot, createdAt, language,
	)
}

func (s *Subscription) handleInteractionCreate(
	ctx context.Context,
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

	s.storage.CreateUser(
		repoDID,
	)
	return s.storage.CreateInteraction(
		ctx, repoDID, uri, cid, kind, createdAt, postUri,
	)
}

func (s *Subscription) handleRecordCreate(
	ctx context.Context,
	repoDID string,
	uri string,
	cid *util.LexLink,
	record typegen.CBORMarshaler,
) error {
	var err error = nil
	switch data := record.(type) {
	case *appbsky.FeedPost:
		err = s.handleFeedPostCreate(
			ctx, repoDID, uri, cid, data,
		)
	case *appbsky.FeedLike:
		err = s.handleInteractionCreate(
			ctx,
			repoDID,
			uri,
			cid,
			db.InteractionTypeLike,
			data.CreatedAt,
			data.Subject.Uri,
		)
	case *appbsky.FeedRepost:
		err = s.handleInteractionCreate(
			ctx,
			repoDID,
			uri,
			cid,
			db.InteractionTypeRepost,
			data.CreatedAt,
			data.Subject.Uri,
		)
	}

	return err
}

func (s *Subscription) handleRecordDelete(
	ctx context.Context,
	uri string,
	recordType string,
) error {
	switch recordType {
	case "app.bsky.feed.post":
		s.storage.DeletePost(ctx, uri)
	case "app.bsky.feed.like", "app.bsky.feed.repost":
		s.storage.DeleteInteraction(ctx, uri)
	}
	return nil
}
