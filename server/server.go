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
		"basque":     "eu",
		"catalan":    "ca",
		"galician":   "gl",
		"portuguese": "pt",
		"spanish":    "es",
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

func (s *Server) Run() {
	http.HandleFunc("/.well-known/did.json", s.getDidJson)
	http.HandleFunc("/xrpc/app.bsky.feed.describeFeedGenerator", s.getDescribeFeedGenerator)
	http.HandleFunc("/xrpc/app.bsky.feed.getFeedSkeleton", s.getFeedSkeleton)

	err := http.ListenAndServe(":3333", nil)
	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}
}

func (s *Server) getDidJson(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	bskyHostname := os.Getenv("BSKY_HOSTNAME")
	serviceDID := "did:web:" + bskyHostname

	jsonResp := utils.ToJson(
		map[string]any{
			"@context": []string{"https://www.w3.org/ns/did/v1"},
			"id":       serviceDID,
			"service": []any{
				map[string]string{
					"id":              "#bsky_fg",
					"type":            "BskyFeedGenerator",
					"serviceEndpoint": "https://" + bskyHostname,
				},
			},
		},
	)
	w.Write(jsonResp)
}

func (s *Server) getDescribeFeedGenerator(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	jsonResp := utils.ToJson(
		map[string]any{
			"encoding": "application/json",
			"body": map[string]any{
				"did": "did:web:" + os.Getenv("BSKY_HOSTNAME"),
				"feeds": []map[string]string{
					{"uri": "at://did:plc:qinqxdwwxgme6r4lgmkry5qu/app.bsky.feed.generator/basque"},
					{"uri": "at://did:plc:qinqxdwwxgme6r4lgmkry5qu/app.bsky.feed.generator/catalan"},
					{"uri": "at://did:plc:qinqxdwwxgme6r4lgmkry5qu/app.bsky.feed.generator/galician"},
					{"uri": "at://did:plc:qinqxdwwxgme6r4lgmkry5qu/app.bsky.feed.generator/portuguese"},
					{"uri": "at://did:plc:qinqxdwwxgme6r4lgmkry5qu/app.bsky.feed.generator/spanish"},
				},
			},
		},
	)
	w.Write(jsonResp)
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
			Cursor: cursor,
		},
	)

	jsonResp := utils.ToJson(result)
	w.Write(jsonResp)
}

func (s *Server) parseUri(uri *string) (string, error) {
	components := strings.Split(*uri, "/")
	repo := strings.Join(components[:len(components)-2], "/")
	entityType := components[len(components)-2]

	if repo != os.Getenv("BSKY_REPO") {
		return "", errors.New("invalid repository")
	}
	if entityType != "app.bsky.feed.generator" {
		return "", errors.New("invalid entity type")
	}

	return components[len(components)-1], nil
}
