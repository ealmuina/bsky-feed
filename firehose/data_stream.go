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
	log "github.com/sirupsen/logrus"
	typegen "github.com/whyrusleeping/cbor-gen"
	"net/url"
	"strings"
	"sync"
)

const SchedulerMaxConcurrency = 8
const PostsToCreateBulkSize = 100
const InteractionsToCreateBulkSize = 100
const PostsToDeleteBulkSize = 100
const InteractionsToDeleteBulkSize = 100

type Subscription struct {
	Service string

	url              url.URL
	connection       *websocket.Conn
	queries          *db.Queries
	languageDetector *utils.LanguageDetector

	userDidCache *sync.Map

	postsToCreateMutex        sync.Mutex
	postsToCreate             []db.BulkCreatePostsParams
	interactionsToCreateMutex sync.Mutex
	interactionsToCreate      []db.BulkCreateInteractionsParams
	postsToDeleteMutex        sync.Mutex
	postsToDelete             []string
	interactionsToDeleteMutex sync.Mutex
	interactionsToDelete      []string
}

func New(service string, queries *db.Queries, url url.URL) *Subscription {
	if cursor := getCursor(service, queries); cursor != 0 {
		url.RawQuery = fmt.Sprintf("cursor=%v", cursor)
	}

	return &Subscription{
		Service: service,

		url:              url,
		connection:       getConnection(url),
		queries:          queries,
		languageDetector: utils.NewLanguageDetector(),

		userDidCache: loadUserDidSeen(queries),

		postsToCreateMutex:        sync.Mutex{},
		postsToCreate:             make([]db.BulkCreatePostsParams, 0, PostsToCreateBulkSize),
		interactionsToCreateMutex: sync.Mutex{},
		interactionsToCreate:      make([]db.BulkCreateInteractionsParams, 0, InteractionsToCreateBulkSize),
		postsToDeleteMutex:        sync.Mutex{},
		postsToDelete:             make([]string, 0, PostsToDeleteBulkSize),
		interactionsToDeleteMutex: sync.Mutex{},
		interactionsToDelete:      make([]string, 0, InteractionsToDeleteBulkSize),
	}
}

func getConnection(url url.URL) *websocket.Conn {
	c, _, err := websocket.DefaultDialer.Dial(url.String(), nil)
	if err != nil {
		log.Error(err)
	}
	return c
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

func loadUserDidSeen(queries *db.Queries) *sync.Map {
	userDidSeen := sync.Map{}

	dids, err := queries.GetUserDids(context.Background())
	if err != nil {
		log.Error(err)
		dids = make([]string, 0)
	}

	for _, did := range dids {
		userDidSeen.Store(did, true)
	}

	return &userDidSeen
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

func (s *Subscription) bulkCreateInteractions(
	ctx context.Context,
	interactions []db.BulkCreateInteractionsParams,
) {
	if _, err := s.queries.BulkCreateInteractions(ctx, interactions); err != nil {
		log.Errorf("Error creating interactions: %s", err)
	}
}

func (s *Subscription) bulkCreatePosts(
	ctx context.Context,
	posts []db.BulkCreatePostsParams,
) {
	if _, err := s.queries.BulkCreatePosts(ctx, posts); err != nil {
		log.Errorf("Error creating posts: %s", err)
	}
}

func (s *Subscription) bulkDeleteInteractions(ctx context.Context, uris []string) {
	if err := s.queries.BulkDeleteInteractions(ctx, uris); err != nil {
		log.Errorf("Error deleting interactions: %s", err)
	}
}

func (s *Subscription) bulkDeletePosts(ctx context.Context, uris []string) {
	if err := s.queries.BulkDeletePosts(ctx, uris); err != nil {
		log.Errorf("Error deleting posts: %s", err)
	}
}

func (s *Subscription) close() {
	err := s.connection.Close()
	if err != nil {
		log.Error(err)
	}
}

func (s *Subscription) createUser(repoDID string) {
	if _, ok := s.userDidCache.Load(repoDID); ok {
		return
	}
	err := s.queries.CreateUser(
		context.Background(),
		db.CreateUserParams{Did: repoDID},
	)
	if err != nil {
		log.Errorf("Error creating user: %s", err)
		return
	}
	s.userDidCache.Store(repoDID, true)
}

func (s *Subscription) deleteInteraction(ctx context.Context, uri string) {
	s.interactionsToDeleteMutex.Lock()
	defer s.interactionsToDeleteMutex.Unlock()

	s.interactionsToDelete = append(s.interactionsToDelete, uri)

	if len(s.interactionsToDelete) >= InteractionsToDeleteBulkSize {
		// Clone buffer and exec bulk delete
		go s.bulkDeleteInteractions(ctx, s.interactionsToDelete)
		// Clear buffer
		s.interactionsToDelete = make([]string, 0, InteractionsToDeleteBulkSize)
	}
}

func (s *Subscription) deletePost(ctx context.Context, uri string) {
	s.postsToDeleteMutex.Lock()
	defer s.postsToDeleteMutex.Unlock()

	s.postsToDelete = append(s.postsToDelete, uri)

	if len(s.postsToDelete) >= PostsToDeleteBulkSize {
		// Clone buffer and exec bulk delete
		go s.bulkDeletePosts(ctx, s.postsToDelete)
		// Clear buffer
		s.postsToDelete = make([]string, 0, PostsToDeleteBulkSize)
	}
}

func (s *Subscription) getHandle() func(context.Context, *events.XRPCStreamEvent) error {
	return func(ctx context.Context, evt *events.XRPCStreamEvent) error {
		if commit := evt.RepoCommit; commit != nil {
			if commit.Seq%100 == 0 {
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
	var replyParent, replyRoot pgtype.Text
	if data.Reply != nil {
		if data.Reply.Parent != nil {
			replyParent = pgtype.Text{String: data.Reply.Parent.Uri, Valid: true}
		}
		if data.Reply.Root != nil {
			replyRoot = pgtype.Text{String: data.Reply.Root.Uri, Valid: true}
		}
	}
	language := s.languageDetector.DetectLanguage(data.Text, data.Langs)

	s.createUser(repoDID)

	s.postsToCreateMutex.Lock()
	defer s.postsToCreateMutex.Unlock()

	s.postsToCreate = append(
		s.postsToCreate,
		db.BulkCreatePostsParams{
			Uri:         uri,
			AuthorDid:   repoDID,
			Cid:         cid.String(),
			ReplyParent: replyParent,
			ReplyRoot:   replyRoot,
			CreatedAt:   pgtype.Timestamp{Time: createdAt, Valid: true},
			Language:    pgtype.Text{String: language, Valid: language != ""},
		},
	)
	if len(s.postsToCreate) >= PostsToCreateBulkSize {
		// Clone buffer and exec bulk insert
		go s.bulkCreatePosts(ctx, s.postsToCreate)
		// Clear buffer
		s.postsToCreate = make([]db.BulkCreatePostsParams, 0, PostsToCreateBulkSize)
	}

	return nil
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

	s.createUser(repoDID)

	s.interactionsToCreateMutex.Lock()
	defer s.interactionsToCreateMutex.Unlock()

	s.interactionsToCreate = append(
		s.interactionsToCreate,
		db.BulkCreateInteractionsParams{
			Uri:       uri,
			Cid:       cid.String(),
			Kind:      kind,
			AuthorDid: repoDID,
			PostUri:   postUri,
			CreatedAt: pgtype.Timestamp{Time: createdAt, Valid: true},
		},
	)
	if len(s.interactionsToCreate) >= InteractionsToCreateBulkSize {
		// Clone buffer and exec bulk insert
		go s.bulkCreateInteractions(ctx, s.interactionsToCreate)
		// Clear buffer
		s.interactionsToCreate = make([]db.BulkCreateInteractionsParams, 0, InteractionsToCreateBulkSize)
	}

	return nil
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
		s.deletePost(ctx, uri)
	case "app.bsky.feed.like", "app.bsky.feed.repost":
		s.deleteInteraction(ctx, uri)
	}
	return nil
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
