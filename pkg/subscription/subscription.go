package subscription

import (
	"bsky/pkg/models"
	"bsky/pkg/utils"
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

func (s FirehoseSubscription) deleteCommit(uri string) {
	s.db.Delete(&models.Post{}, "uri = ?", uri)
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

		if rec == nil || !strings.HasPrefix(path, "app.bsky.feed.post") {
			return nil
		}
		post := rec.(*appbsky.FeedPost)
		uri := fmt.Sprintf("at://%v/%v", did, path)

		switch op {
		case repomgr.EvtKindCreateRecord:
			{
				s.processCommit(post, uri, rcid.String())
			}
		case repomgr.EvtKindDeleteRecord:
			{
				s.deleteCommit(uri)
			}
		}
		return nil
	}
}

func (s FirehoseSubscription) processCommit(post *appbsky.FeedPost, uri string, cid string) {
	text := strings.ToLower(post.Text)
	if utils.ContainsAnySubstring(text, "bluesky", " bsky") {
		var replyParent, replyRoot *string

		if post.Reply != nil {
			replyParent = &post.Reply.Parent.Uri
			replyRoot = &post.Reply.Root.Uri
		}

		s.db.Create(&models.Post{
			Uri:         uri,
			Cid:         cid,
			ReplyParent: replyParent,
			ReplyRoot:   replyRoot,
			IndexedAt:   time.Now(),
		})
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
