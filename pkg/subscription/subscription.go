package subscription

import (
	"bsky/pkg/models"
	"context"
	"fmt"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/gorilla/websocket"
	"github.com/ipfs/go-cid"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"net/url"
	"strings"
	"sync"
	"time"
)

type FirehoseSubscription struct {
	Service    string
	connection *websocket.Conn
	db         *gorm.DB
}

func New(service string, db *gorm.DB, url url.URL) *FirehoseSubscription {
	c, _, err := websocket.DefaultDialer.Dial(url.String(), nil)
	if err != nil {
		log.Error(err)
		return nil
	}
	return &FirehoseSubscription{
		Service:    service,
		connection: c,
		db:         db,
	}
}

func (s FirehoseSubscription) Run() {
	defer s.close()
	err := events.ConsumeRepoStreamLite(context.Background(), s.connection, s.getHandle())
	if err != nil {
		log.Error(err)
	}
}

func (s FirehoseSubscription) close() {
	err := s.connection.Close()
	if err != nil {
		log.Error(err)
	}
}

func (s FirehoseSubscription) getCursor() int {
	var subState models.SubState
	s.db.Where("service = ?", s.Service).First(&subState)
	return subState.Cursor
}

func (s FirehoseSubscription) getHandle() events.LiteStreamHandleFunc {
	mu := sync.Mutex{}
	i := 0 // counter for updating cursor every 20 events

	return func(op repomgr.EventKind, seq int64, path string, did string, rcid *cid.Cid, rec any) error {
		mu.Lock()
		i++
		if i%20 == 0 {
			go s.updateCursor(i)
		}
		mu.Unlock()

		uri := fmt.Sprintf("at://%v/%v", did, path)

		if strings.HasPrefix(path, "app.bsky.feed.like") {
			s.processLike(rec, uri, did, rcid, op)
		}

		if strings.HasPrefix(path, "app.bsky.feed.post") {
			s.processPost(rec, uri, rcid, op)
		}

		return nil
	}
}

func (s FirehoseSubscription) processLike(rec any, uri string, actorDid string, rcid *cid.Cid, op repomgr.EventKind) {
	switch op {
	case repomgr.EvtKindCreateRecord:
		{
			like := rec.(*appbsky.FeedLike)

			var post models.Post
			result := s.db.Where("uri = ?", like.Subject.Uri).First(&post)

			if result.RowsAffected == 1 {
				s.db.Create(&models.Like{
					Uri:       uri,
					Cid:       rcid.String(),
					ActorDid:  actorDid,
					Post:      post,
					IndexedAt: time.Now(),
				})
			}
		}
	case repomgr.EvtKindDeleteRecord:
		{
			s.db.Delete(&models.Like{}, "uri = ?", uri)
		}
	}
}

func (s FirehoseSubscription) processPost(rec any, uri string, rcid *cid.Cid, op repomgr.EventKind) {
	switch op {
	case repomgr.EvtKindCreateRecord:
		{
			post := rec.(*appbsky.FeedPost)

			var replyParent, replyRoot *string

			if post.Reply != nil {
				replyParent = &post.Reply.Parent.Uri
				replyRoot = &post.Reply.Root.Uri
			}

			s.db.Create(&models.Post{
				Uri:         uri,
				Cid:         rcid.String(),
				ReplyParent: replyParent,
				ReplyRoot:   replyRoot,
				IndexedAt:   time.Now(),
			})
		}
	case repomgr.EvtKindDeleteRecord:
		{
			s.db.Delete(&models.Post{}, "uri = ?", uri)
		}
	}
}

func (s FirehoseSubscription) updateCursor(cursor int) {
	s.db.Model(
		models.SubState{},
	).Select(
		"cursor",
	).Where(
		"service = ?", s.Service,
	).Updates(
		models.SubState{Cursor: cursor},
	)
}
