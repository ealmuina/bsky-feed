package server

import (
	"bsky/cache"
	db "bsky/db/sqlc"
	"bsky/feeds"
	"bsky/feeds/algorithms"
	"bsky/utils"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
)

var LanguageFeeds = map[string]string{
	"basque":     "eu",
	"catalan":    "ca",
	"galician":   "gl",
	"portuguese": "pt",
	"spanish":    "es",
}
var TopLanguageFeeds = map[string]string{
	"top_spanish": "es",
}

type Server struct {
	queries *db.Queries
	feeds   map[string]*feeds.Feed
}

func NewServer(
	queries *db.Queries,
	timelinesCache *cache.TimelinesCache,
	usersCache *cache.UsersCache,
) Server {
	serverFeeds := make(map[string]*feeds.Feed)

	// Populate language feeds
	for language, languageCode := range LanguageFeeds {
		serverFeeds[language] = feeds.NewFeed(
			language,
			queries,
			timelinesCache,
			usersCache,
			algorithms.GetLanguageAlgorithm(languageCode),
			algorithms.GetLanguageAlgorithmAcceptance(languageCode),
		)
	}
	// Additional feeds
	for feedName, languageCode := range TopLanguageFeeds {
		serverFeeds[feedName] = feeds.NewFeed(
			feedName,
			queries,
			timelinesCache,
			usersCache,
			algorithms.GetTopLanguageAlgorithm(languageCode),
			algorithms.GetTopLanguageAlgorithmAcceptance(languageCode),
		)
	}

	return Server{
		queries: queries,
		feeds:   serverFeeds,
	}
}

func (s *Server) GetFeeds() []*feeds.Feed {
	result := make([]*feeds.Feed, 0, len(s.feeds))
	for _, feed := range s.feeds {
		result = append(result, feed)
	}
	return result
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
					{"uri": "at://did:plc:qinqxdwwxgme6r4lgmkry5qu/app.bsky.feeds.generator/basque"},
					{"uri": "at://did:plc:qinqxdwwxgme6r4lgmkry5qu/app.bsky.feeds.generator/catalan"},
					{"uri": "at://did:plc:qinqxdwwxgme6r4lgmkry5qu/app.bsky.feeds.generator/galician"},
					{"uri": "at://did:plc:qinqxdwwxgme6r4lgmkry5qu/app.bsky.feeds.generator/portuguese"},
					{"uri": "at://did:plc:qinqxdwwxgme6r4lgmkry5qu/app.bsky.feeds.generator/spanish"},
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
	result := requestedFeed.GetTimeline(
		feeds.QueryParams{
			Limit:  int64(limit),
			Cursor: *cursor,
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
