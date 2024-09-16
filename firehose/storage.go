package firehose

import (
	db "bsky/db/sqlc"
	"context"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

const PostsToCreateBulkSize = 100
const InteractionsToCreateBulkSize = 100
const PostsToDeleteBulkSize = 100
const InteractionsToDeleteBulkSize = 100

type Storage struct {
	queries *db.Queries

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

func NewStorage(queries *db.Queries) Storage {
	return Storage{
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

func (s *Storage) CreateInteraction(
	ctx context.Context,
	authorDid string,
	uri string,
	cid *util.LexLink,
	kind db.InteractionType,
	createdAt time.Time,
	postUri string,
) error {
	s.interactionsToCreateMutex.Lock()
	defer s.interactionsToCreateMutex.Unlock()

	s.interactionsToCreate = append(
		s.interactionsToCreate,
		db.BulkCreateInteractionsParams{
			Uri:       uri,
			Cid:       cid.String(),
			Kind:      kind,
			AuthorDid: authorDid,
			PostUri:   postUri,
			CreatedAt: pgtype.Timestamp{Time: createdAt, Valid: true},
		},
	)
	if len(s.interactionsToCreate) >= InteractionsToCreateBulkSize {
		// Clone buffer and exec bulk insert
		go func(interactions []db.BulkCreateInteractionsParams) {
			if _, err := s.queries.BulkCreateInteractions(ctx, interactions); err != nil {
				log.Errorf("Error creating interactions: %s", err)
			}
		}(s.interactionsToCreate)
		// Clear buffer
		s.interactionsToCreate = make([]db.BulkCreateInteractionsParams, 0, InteractionsToCreateBulkSize)
	}

	return nil
}

func (s *Storage) CreatePost(
	ctx context.Context,
	uri string,
	authorDid string,
	cid *util.LexLink,
	replyParent string,
	replyRoot string,
	createdAt time.Time,
	language string,
) error {
	s.postsToCreateMutex.Lock()
	defer s.postsToCreateMutex.Unlock()

	s.postsToCreate = append(
		s.postsToCreate,
		db.BulkCreatePostsParams{
			Uri:         uri,
			AuthorDid:   authorDid,
			Cid:         cid.String(),
			ReplyParent: pgtype.Text{String: replyParent, Valid: replyParent != ""},
			ReplyRoot:   pgtype.Text{String: replyRoot, Valid: replyRoot != ""},
			CreatedAt:   pgtype.Timestamp{Time: createdAt, Valid: true},
			Language:    pgtype.Text{String: language, Valid: language != ""},
		},
	)
	if len(s.postsToCreate) >= PostsToCreateBulkSize {
		// Clone buffer and exec bulk insert
		go func(posts []db.BulkCreatePostsParams) {
			if _, err := s.queries.BulkCreatePosts(ctx, posts); err != nil {
				log.Errorf("Error creating posts: %s", err)
			}
		}(s.postsToCreate)
		// Clear buffer
		s.postsToCreate = make([]db.BulkCreatePostsParams, 0, PostsToCreateBulkSize)
	}

	return nil
}

func (s *Storage) CreateUser(repoDID string) {
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

func (s *Storage) DeleteInteraction(ctx context.Context, uri string) {
	s.interactionsToDeleteMutex.Lock()
	defer s.interactionsToDeleteMutex.Unlock()

	s.interactionsToDelete = append(s.interactionsToDelete, uri)

	if len(s.interactionsToDelete) >= InteractionsToDeleteBulkSize {
		// Copy buffer and exec bulk delete
		go func(uris []string) {
			if err := s.queries.BulkDeleteInteractions(ctx, uris); err != nil {
				log.Errorf("Error deleting interactions: %s", err)
			}
		}(s.interactionsToDelete)
		// Clear buffer
		s.interactionsToDelete = make([]string, 0, InteractionsToDeleteBulkSize)
	}
}

func (s *Storage) DeletePost(ctx context.Context, uri string) {
	s.postsToDeleteMutex.Lock()
	defer s.postsToDeleteMutex.Unlock()

	s.postsToDelete = append(s.postsToDelete, uri)

	if len(s.postsToDelete) >= PostsToDeleteBulkSize {
		// Copy buffer and exec bulk delete
		go func(uris []string) {
			if err := s.queries.BulkDeletePosts(ctx, uris); err != nil {
				log.Errorf("Error deleting posts: %s", err)
			}
		}(s.postsToDelete)
		// Clear buffer
		s.postsToDelete = make([]string, 0, PostsToDeleteBulkSize)
	}
}

func (s *Storage) GetCursor(service string) int64 {
	state, err := s.queries.GetSubscriptionState(
		context.Background(),
		service,
	)
	if err != nil {
		log.Errorf("Error getting subscription state: %s", err)
	}
	return state.Cursor
}

func (s *Storage) UpdateCursor(service string, cursor int64) {
	err := s.queries.UpdateSubscriptionStateCursor(
		context.Background(),
		db.UpdateSubscriptionStateCursorParams{
			Cursor:  cursor,
			Service: service,
		},
	)
	if err != nil {
		log.Errorf("Error updating cursor: %s", err)
	}
}
