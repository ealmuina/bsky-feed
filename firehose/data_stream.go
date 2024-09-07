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
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pemistahl/lingua-go"
	log "github.com/sirupsen/logrus"
	typegen "github.com/whyrusleeping/cbor-gen"
	"net/url"
)

const PostsToCreateQueueSize = 100

type Subscription struct {
	Service          string
	connection       *websocket.Conn
	queries          *db.Queries
	languageDetector *lingua.LanguageDetector

	userDidSeen   map[string]bool
	postsToCreate []db.BulkCreatePostsParams
}

func getCursor(service string, queries *db.Queries) int64 {
	state, err := queries.GetSubscriptionState(
		context.Background(),
		service,
	)
	if err != nil {
		log.Errorf("Error getting subscription state: %s", err)
	}
	return state.Cursor
}

func New(service string, queries *db.Queries, url url.URL) *Subscription {
	url.RawQuery = fmt.Sprintf("cursor=%v", getCursor(service, queries))

	c, _, err := websocket.DefaultDialer.Dial(url.String(), nil)
	if err != nil {
		log.Error(err)
		return nil
	}

	detector := lingua.NewLanguageDetectorBuilder().
		FromAllLanguages().
		WithPreloadedLanguageModels().
		Build()

	userDidSeen := make(map[string]bool)
	dids, err := queries.GetUserDids(context.Background())
	if err != nil {
		log.Error(err)
		dids = make([]string, 0)
	}
	for _, did := range dids {
		userDidSeen[did] = true
	}

	return &Subscription{
		Service:          service,
		connection:       c,
		queries:          queries,
		languageDetector: &detector,
	}
}

func (s *Subscription) Run() {
	defer s.close()

	scheduler := parallel.NewScheduler(
		4,
		1000,
		"data_stream",
		s.getHandle(),
	)

	err := events.HandleRepoStream(
		context.Background(),
		s.connection,
		scheduler,
	)
	if err != nil {
		log.Panic(err)
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
			if commit.Seq%20 == 0 {
				go s.updateCursor(commit.Seq)
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
					if err := s.handleRecordDelete(
						ctx, commit.Repo, uri,
					); err != nil {
						log.Errorf("Error handling delete record: %s", err)
						return err
					}
				}
			}
		}
		return nil
	}
}

func (s *Subscription) handleRecordCreate(
	ctx context.Context,
	repoDID string,
	uri string,
	cid *util.LexLink,
	record typegen.CBORMarshaler,
) error {
	switch data := record.(type) {
	case *appbsky.FeedPost:
		err := s.handleFeedPostCreate(
			ctx, repoDID, uri, cid, data,
		)
		if err != nil {
			log.Errorf("Error handling feed post create: %s", err)
			return err
		}
	default:
		// Ignore event
	}
	return nil
}

func (s *Subscription) handleRecordDelete(
	ctx context.Context,
	repo string,
	uri string,
) error {
	return nil
}

func (s *Subscription) handleFeedPostCreate(
	ctx context.Context,
	repoDID string,
	uri string,
	cid *util.LexLink,
	data *appbsky.FeedPost,
) error {
	s.createUser(repoDID)

	createdAt, err := utils.ParseTime(data.CreatedAt)
	if err != nil {
		log.Errorf("Error parsing created at: %s", err)
		return err
	}

	var replyParent, replyRoot pgtype.Text
	if data.Reply != nil {
		replyParent = pgtype.Text{String: data.Reply.Parent.Uri, Valid: true}
		replyRoot = pgtype.Text{String: data.Reply.Root.Uri, Valid: true}
	}

	s.postsToCreate = append(
		s.postsToCreate,
		db.BulkCreatePostsParams{
			Uri:         uri,
			AuthorDid:   pgtype.Text{String: repoDID, Valid: true},
			Cid:         cid.String(),
			ReplyParent: replyParent,
			ReplyRoot:   replyRoot,
			CreatedAt:   pgtype.Timestamp{Time: createdAt, Valid: true},
		},
	)

	if len(s.postsToCreate) > PostsToCreateQueueSize {
		// Bulk create posts
		if _, err := s.queries.BulkCreatePosts(ctx, s.postsToCreate); err != nil {
			log.Errorf("Error creating posts: %s", err)
			return err
		}

		// Clear posts to create queue
		s.postsToCreate = make([]db.BulkCreatePostsParams, 0, PostsToCreateQueueSize)
	}

	return nil
}

func (s *Subscription) createUser(repoDID string) {
	if s.userDidSeen[repoDID] {
		return
	}
	err := s.queries.CreateUser(
		context.Background(),
		db.CreateUserParams{Did: repoDID},
	)
	if err != nil {
		log.Errorf("Error creating user: %s", err)
	}
}

func (s *Subscription) updateCursor(cursor int64) {
	err := s.queries.UpdateSubscriptionStateCursor(
		context.Background(),
		db.UpdateSubscriptionStateCursorParams{
			Cursor:  cursor,
			Service: s.Service,
		},
	)
	if err != nil {
		log.Errorf("Error updating cursor: %s", err)
	}
}
