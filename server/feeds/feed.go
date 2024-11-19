package feeds

import (
	"bsky/storage"
	"bsky/storage/models"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"time"
)

const CursorEOF = "eof"

type Feed struct {
	name           string
	storageManager *storage.Manager
}

func getNewCursor() string {
	return fmt.Sprintf("%d", time.Now().Unix())
}

func NewFeed(name string, storageManager *storage.Manager) Feed {
	return Feed{
		name:           name,
		storageManager: storageManager,
	}
}

func (f *Feed) GetTimeline(params QueryParams) Response {
	if params.Cursor == "" {
		params.Cursor = getNewCursor()
	} else if params.Cursor == CursorEOF {
		return Response{
			Cursor: CursorEOF,
			Posts:  make([]models.TimelineEntry, 0),
		}
	}

	cursorRank, err := strconv.ParseFloat(params.Cursor, 64)
	if err != nil {
		log.Errorf("Malformed cursor in %+v", params)
		return Response{}
	}

	entries := f.storageManager.GetTimeline(f.name, cursorRank, params.Limit)

	cursor := CursorEOF
	if len(entries) > 0 {
		lastPost := entries[len(entries)-1]
		cursor = fmt.Sprintf("%f", lastPost.Rank)
	}

	return Response{
		Cursor: cursor,
		Posts:  entries,
	}
}
