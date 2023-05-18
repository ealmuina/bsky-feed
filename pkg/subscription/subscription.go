package subscription

import (
	"bsky/pkg/models"
	"bsky/pkg/utils"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/repo"
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

		if !strings.HasPrefix(path, "app.bsky.feed.post") {
			return nil
		}
		post := rec.(appbsky.FeedPost)
		uri := fmt.Sprintf("at://%v/%v", did, path)

		switch op {
		case repomgr.EvtKindCreateRecord:
			{
				s.processCommit(&post, uri, rcid.String())
			}
		case repomgr.EvtKindDeleteRecord:
			{
				s.deleteCommit(uri)
			}
		}
		return nil
	}
}

func (s FirehoseSubscription) Run() {
	defer s.close()
	err := events.ConsumeRepoStreamLite(context.Background(), s.connection, s.getHandle())
	//err := events.HandleRepoStream(context.Background(), s.connection, &events.RepoStreamCallbacks{
	//	RepoCommit: s.getHandleCommit(),
	//	RepoInfo:   s.getHandleInfo(),
	//	Error:      s.getHandleError(),
	//})
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

func (s FirehoseSubscription) getHandleCommit() func(*comatproto.SyncSubscribeRepos_Commit) error {
	mu := sync.Mutex{}
	i := 0 // counter for updating cursor every 20 events

	return func(evt *comatproto.SyncSubscribeRepos_Commit) error {
		mu.Lock()
		i++
		if i%20 == 0 {
			go s.updateCursor(i)
		}
		mu.Unlock()

		ctx := context.Background()
		rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
		if err != nil {
			return err
		}

		for _, op := range evt.Ops {
			if !strings.HasPrefix(op.Path, "app.bsky.feed.post") {
				continue
			}

			ek := repomgr.EventKind(op.Action)
			switch ek {
			case repomgr.EvtKindCreateRecord, repomgr.EvtKindUpdateRecord:
				{
					// Grab the record from the merkel tree
					rc, rec, err := rr.GetRecord(ctx, op.Path)
					if err != nil {
						return err
					}

					// Verify that the record cid matches the cid in the event
					if lexutil.LexLink(rc) != *op.Cid {
						return errors.New(fmt.Sprintf("mismatch in record and op cid: %s != %s", rc, *op.Cid))
					}

					recordAsCAR := lexutil.LexiconTypeDecoder{
						Val: rec,
					}

					// Attempt to Unpack the CAR Blocks into JSON Byte Array
					b, err := recordAsCAR.MarshalJSON()
					if err != nil {
						return err
					}

					// Unmarshal the JSON Byte Array into a FeedPost
					var pst = appbsky.FeedPost{}
					err = json.Unmarshal(b, &pst)
					s.processCommit(op, &pst)

					if err != nil {
						return err
					}
				}
			case repomgr.EvtKindDeleteRecord:
				{
					s.deleteCommit(op)
				}
			}
		}

		return nil
	}
}

func (s FirehoseSubscription) getHandleError() func(*events.ErrorFrame) error {
	return func(evt *events.ErrorFrame) error {
		log.Warn(fmt.Sprintf("%v: %v", evt.Error, evt.Message))
		return nil
	}
}

func (s FirehoseSubscription) getHandleInfo() func(*comatproto.SyncSubscribeRepos_Info) error {
	return func(evt *comatproto.SyncSubscribeRepos_Info) error {
		fmt.Printf("%+v\n", evt)
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
