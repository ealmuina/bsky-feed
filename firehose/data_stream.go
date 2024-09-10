package firehose

import (
	db "bsky/db/sqlc"
	"bsky/tasks"
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

type Subscription struct {
	Service string

	url               url.URL
	connection        *websocket.Conn
	queries           *db.Queries
	languageDetector  *utils.LanguageDetector
	statisticsUpdater *tasks.StatisticsUpdater

	userDidSeen     *sync.Map
	languageIdCache *sync.Map
}

func New(service string, queries *db.Queries, url url.URL) *Subscription {
	if cursor := getCursor(service, queries); cursor != 0 {
		url.RawQuery = fmt.Sprintf("cursor=%v", cursor)
	}

	userDidSeen := sync.Map{}
	dids, err := queries.GetUserDids(context.Background())
	if err != nil {
		log.Error(err)
		dids = make([]string, 0)
	}
	for _, did := range dids {
		userDidSeen.Store(did, true)
	}

	statisticsUpdater, err := tasks.NewStatisticsUpdater(queries)
	if err != nil {
		log.Errorf("Error creating statistics updater: %v", err)
		panic(err)
	}

	return &Subscription{
		Service: service,

		url:               url,
		connection:        getConnection(url),
		queries:           queries,
		languageDetector:  utils.NewLanguageDetector(),
		statisticsUpdater: statisticsUpdater,

		userDidSeen:     &userDidSeen,
		languageIdCache: &sync.Map{},
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

func (s *Subscription) Run() {
	defer s.close()

	scheduler := parallel.NewScheduler(
		4,
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

func (s *Subscription) createPost(
	ctx context.Context,
	post *db.CreatePostParams,
	languages []string,
) {
	postLanguages := make([]db.BulkCreatePostLanguagesParams, len(languages))
	for _, lang := range languages {
		postLanguages = append(
			postLanguages,
			db.BulkCreatePostLanguagesParams{
				PostUri:    pgtype.Text{String: post.Uri, Valid: true},
				LanguageID: pgtype.Int4{Int32: s.getLanguageId(lang), Valid: true},
			},
		)
	}

	// Create post
	if err := s.queries.CreatePost(ctx, *post); err != nil {
		log.Errorf("Error creating posts: %s", err)
	}
	// Create post languages
	if _, err := s.queries.BulkCreatePostLanguages(ctx, postLanguages); err != nil {
		log.Errorf("Error creating post languages: %s", err)
	}
}

func (s *Subscription) createUser(repoDID string) {
	if _, ok := s.userDidSeen.Load(repoDID); ok {
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
	s.userDidSeen.Store(repoDID, true)
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

func (s *Subscription) getLanguageId(languageCode string) int32 {
	languageCode = strings.ToLower(languageCode)

	if languageId, ok := s.languageIdCache.Load(languageCode); ok {
		return languageId.(int32)
	}

	languageId, err := s.queries.GetLanguage(
		context.Background(),
		languageCode,
	)
	if err != nil {
		languageId, err = s.queries.CreateLanguage(
			context.Background(),
			languageCode,
		)
		if err != nil {
			log.Errorf("Error creating language: %s", err)
			return -1
		}
	}
	s.languageIdCache.Store(languageCode, languageId)

	return languageId
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
	languages := s.languageDetector.DetectLanguage(data.Text, data.Langs)

	go s.createPost(
		ctx,
		&db.CreatePostParams{
			Uri:         uri,
			AuthorDid:   pgtype.Text{String: repoDID, Valid: true},
			Cid:         cid.String(),
			ReplyParent: replyParent,
			ReplyRoot:   replyRoot,
			CreatedAt:   pgtype.Timestamp{Time: createdAt, Valid: true},
		},
		languages,
	)
	//s.statisticsUpdater.UpdateUserStatistics(repoDID)

	return nil
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
	}
	return nil
}

func (s *Subscription) handleRecordDelete(
	ctx context.Context,
	uri string,
	recordType string,
) error {
	switch recordType {
	case "app.bsky.feed.post":
		if err := s.queries.DeletePost(ctx, uri); err != nil {
			log.Errorf("Error deleting post: %s", err)
			return err
		}
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
