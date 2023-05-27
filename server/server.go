package server

import (
	"bsky/pkg/algorithms"
	"bsky/pkg/auth"
	"bsky/pkg/feed"
	"bsky/pkg/utils"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"net/http"
	"os"
)

type FeedAlgorithm = func(*auth.AuthConfig, *gorm.DB, feed.QueryParams) (string, []feed.SkeletonItem)

type Server struct {
	DB *gorm.DB
}

func (s Server) getFeedSkeleton(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	queryParams := r.URL.Query()
	feedUri := getQueryItem(queryParams, "feed")
	cursor := getQueryItem(queryParams, "cursor")
	limit := getQueryItem(queryParams, "limit")

	feedAlgo := map[string]FeedAlgorithm{
		algorithms.SpanishUri: algorithms.Spanish,
	}[*feedUri]

	if feedAlgo == nil {
		sendError(w, http.StatusNotFound, "feed not found")
		return
	}

	nextCursor, items := feedAlgo(nil, s.DB, feed.QueryParams{
		Cursor: cursor,
		Limit:  utils.IntFromString(*limit, 100),
	})

	resp := map[string]any{
		"feed": items,
	}
	if nextCursor != "" {
		resp["cursor"] = nextCursor
	}

	jsonResp := utils.ToJson(resp)
	w.Write(jsonResp)
}

func (s Server) Run() {
	http.HandleFunc("/xrpc/app.bsky.feed.getFeedSkeleton", s.getFeedSkeleton)

	err := http.ListenAndServe(":3000", nil)
	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}
}
