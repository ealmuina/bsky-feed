package algorithms

import (
	"bsky/storage/cache"
	"bsky/storage/models"
	"os"
	"strconv"
)

type Algorithm interface {
	AcceptsPost(post models.Post, authorStatistics cache.UserStatistics) (ok bool, reason map[string]string)
}

var ImplementedAlgorithms = map[string]Algorithm{
	"basque":         &LanguageAlgorithm{"eu"},
	"catalan":        &LanguageAlgorithm{"ca"},
	"galician":       &LanguageAlgorithm{"gl"},
	"portuguese":     &LanguageAlgorithm{"pt"},
	"spanish":        &LanguageAlgorithm{"es"},
	"chinese":        &LanguageAlgorithm{"zh"},
	"top_spanish":    newTopLanguageAlgorithm("es"),
	"top_portuguese": newTopLanguageAlgorithm("pt"),
	"light_spanish":  &LightLanguageAlgorithm{"es"},
}

func newTopLanguageAlgorithm(lang string) *TopLanguageAlgorithm {
	return &TopLanguageAlgorithm{
		languageCode:      lang,
		minFollowers:      int64(intFromEnv("TOP_FEED_MIN_FOLLOWERS", 1000)),
		minEngagement:     floatFromEnv("TOP_FEED_MIN_ENGAGEMENT", 1.0),
		engagementDivisor: floatFromEnv("TOP_FEED_ENGAGEMENT_DIVISOR", 1.0),
		maxFollowsRatio:   floatFromEnv("TOP_FEED_MAX_FOLLOWS_RATIO", 5.0),
		minAccountAgeDays: int64(intFromEnv("TOP_FEED_MIN_ACCOUNT_AGE_DAYS", 7)),
		minTextLength:     intFromEnv("TOP_FEED_MIN_TEXT_LENGTH", 20),
		maxUppercaseRatio: floatFromEnv("TOP_FEED_MAX_UPPERCASE_RATIO", 0.7),
	}
}

func intFromEnv(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func floatFromEnv(key string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil {
		return v
	}
	return def
}
