package algorithms

import (
	"bsky/storage/cache"
	"bsky/storage/models"
	"strings"
	"time"
	"unicode"
)

type TopLanguageAlgorithm struct {
	languageCode      string
	minFollowers      int64
	minEngagement     float64
	engagementDivisor float64
	maxFollowsRatio   float64
	minAccountAgeDays int64
	minTextLength     int
	maxUppercaseRatio float64
}

func (a *TopLanguageAlgorithm) AcceptsPost(
	post models.Post,
	authorStatistics cache.UserStatistics,
) (ok bool, reason map[string]string) {
	if post.Language != a.languageCode {
		return
	}
	if post.ReplyRootId != 0 {
		return
	}
	if authorStatistics.FollowersCount <= a.minFollowers {
		return
	}
	if authorStatistics.GetEngagementFactor(a.engagementDivisor) <= a.minEngagement {
		return
	}

	// Bot heuristic: far more follows than followers
	if authorStatistics.FollowersCount > 0 &&
		float64(authorStatistics.FollowsCount) > a.maxFollowsRatio*float64(authorStatistics.FollowersCount) {
		return
	}

	// Account age: let unknown (zero) through; reject only if we know the account is too new
	if !authorStatistics.CreatedAt.IsZero() {
		if time.Since(authorStatistics.CreatedAt) < time.Duration(a.minAccountAgeDays)*24*time.Hour {
			return
		}
	}

	// Minimum content length (rune-aware, ignores surrounding whitespace)
	if a.minTextLength > 0 && runeLen(strings.TrimSpace(post.Text)) < a.minTextLength {
		return
	}

	// Shouting filter: skip if too few letters to be meaningful
	if a.maxUppercaseRatio > 0 && uppercaseRatio(post.Text) > a.maxUppercaseRatio {
		return
	}

	ok = true
	return
}

func runeLen(s string) int {
	return len([]rune(s))
}

func uppercaseRatio(s string) float64 {
	var letters, upper int
	for _, r := range s {
		if unicode.IsLetter(r) {
			letters++
			if unicode.IsUpper(r) {
				upper++
			}
		}
	}
	if letters < 10 {
		return 0
	}
	return float64(upper) / float64(letters)
}
