package server

import (
	db "bsky/db/sqlc"
	"bsky/feed"
	"bsky/feed/algorithms"
	"bsky/utils"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
)

type Server struct {
	queries *db.Queries
	feeds   map[string]*feed.Feed
}

func New(queries *db.Queries) Server {
	feeds := make(map[string]*feed.Feed)

	// Populate language feeds
	languages := map[string]string{
		"spanish":    "es",
		"portuguese": "pt",
		"catalan":    "ca",
	}
	for language, languageCode := range languages {
		f := feed.New(
			queries,
			algorithms.GetLanguageAlgorithm(languageCode),
		)
		feeds[language] = &f
	}

	return Server{
		queries: queries,
		feeds:   feeds,
	}
}

func (s *Server) getFeedSkeleton(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	queryParams := r.URL.Query()
	feedUri := getQueryItem(queryParams, "feed")
	cursor := getQueryItem(queryParams, "cursor")

	limitStr := getQueryItem(queryParams, "limit")
	limit := 100
	if limitStr != nil {
		parsedLimit, err := strconv.Atoi(*limitStr)
		if err != nil {
			sendError(w, http.StatusBadRequest, "invalid limit param")
			return
		}
		limit = parsedLimit
	}

	feedName, err := s.parseUri(feedUri)
	if err != nil || s.feeds[feedName] == nil {
		sendError(w, http.StatusNotFound, "feed not found")
		return
	}

	requestedFeed := s.feeds[feedName]
	result := requestedFeed.GetPosts(
		feed.QueryParams{
			Limit:  limit,
			Cursor: *cursor,
		},
	)

	jsonResp := utils.ToJson(result)
	w.Write(jsonResp)
}

func (s *Server) Run() {
	http.HandleFunc("/xrpc/app.bsky.feed.getFeedSkeleton", s.getFeedSkeleton)

	err := http.ListenAndServe(":3333", nil)
	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}
}

func (s *Server) parseUri(uri *string) (string, error) {
	components := strings.Split(*uri, "/")

	//TODO: Validate uri

	return components[len(components)-1], nil
}
