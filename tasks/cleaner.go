package tasks

import (
	"bsky/storage"
	"bsky/utils"
	"math"
	"os"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

// Cleaner interval defaults. The DB pass is expensive (multi-batch
// DELETEs on a large table) so it runs hourly; the timeline/cache pass
// is cheap (Redis ZREMRANGEBYSCORE + a 10k-row SELECT) and can run
// every minute. Both intervals are overridable via env vars so the
// same image can be retuned without rebuilding.
const (
	defaultCleanerDBInterval       = 1 * time.Hour
	defaultCleanerTimelineInterval = 1 * time.Minute
)

// CleanOldData launches two independent cleaner goroutines with
// individually-configurable intervals. They are intentionally NOT
// serialized into a single ticker: the timeline/cache pass is cheap
// and can run every minute, while the DB-delete pass is expensive and
// should run hourly by default. Each goroutine has its own Recoverer
// so a panic in one does not stop the other.
//
// `persistentDb` mirrors the RUN_BACKFILL env var: when true, the DB
// cleaner is skipped (the backfill needs the historical data), but the
// cheap Redis-only cleaner still runs.
func CleanOldData(storageManager *storage.Manager, persistentDb bool) {
	dbInterval := parseCleanerInterval("CLEANER_DB_INTERVAL", defaultCleanerDBInterval)
	timelineInterval := parseCleanerInterval("CLEANER_TIMELINE_INTERVAL", defaultCleanerTimelineInterval)

	// Timeline + cache pass (cheap, runs often). Always on, even in
	// backfill mode — Redis-only and the backfill tolerates it.
	go utils.Recoverer(math.MaxInt, 1, func() {
		runCleanerLoop("timeline+cache", timelineInterval, func() {
			storageManager.CleanTimelinesAndCaches()
		})
	})

	// DB pass. Skipped when the DB is being used for backfill.
	if persistentDb {
		return
	}
	if dbInterval == 0 {
		log.Warn("CLEANER_DB_INTERVAL=0, DB cleaner disabled (NOT recommended in production)")
		return
	}
	go utils.Recoverer(math.MaxInt, 2, func() {
		runCleanerLoop("db", dbInterval, func() {
			storageManager.CleanOldData(persistentDb)
		})
	})
}

// runCleanerLoop runs `fn` every `interval` until the process exits.
// Uses time.NewTimer (not time.After) to avoid the well-known
// goroutine leak: every tick of a time.After-based loop leaks the
// channel until GC, and the timer cannot be stopped on early exit.
func runCleanerLoop(name string, interval time.Duration, fn func()) {
	t := time.NewTimer(interval)
	defer t.Stop()
	for {
		<-t.C
		fn()
		t.Reset(interval)
	}
}

// parseCleanerInterval accepts Go duration syntax ("1h", "30m", "15s")
// or bare seconds ("3600"). Falls back to the default with a warning
// on parse error.
func parseCleanerInterval(envName string, defaultValue time.Duration) time.Duration {
	raw := os.Getenv(envName)
	if raw == "" {
		return defaultValue
	}
	if d, err := time.ParseDuration(raw); err == nil {
		return d
	}
	if secs, err := strconv.Atoi(raw); err == nil {
		return time.Duration(secs) * time.Second
	}
	log.Warnf("Invalid %s=%q, using default %s", envName, raw, defaultValue)
	return defaultValue
}
