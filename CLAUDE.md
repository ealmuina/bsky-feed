# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A custom feed generator service for the Bluesky social network. It subscribes to the Bluesky firehose (Jetstream),
detects post language via fastText, stores data in PostgreSQL + Redis, and serves language-specific feeds (Spanish,
Portuguese, Catalan, Basque, Galician, Chinese).

## Commands

```bash
# Run locally
go run .

# Full stack (app + postgres + redis + prometheus + grafana)
docker-compose up

# Startup script (downloads fasttext model, runs migrations, starts app)
./run.sh

# Run all tests
go test ./...

# Run tests for a specific package (e.g. algorithms — no C deps, runs anywhere)
go test ./storage/algorithms/...

# Run a single test by name
go test ./storage/algorithms/... -run TestTopLanguage_BotFollowsRatio

# Regenerate SQLC query code (after editing /storage/db/queries/*.sql)
sqlc generate

# Apply DB migrations manually
migrate -path storage/db/migrations -database "postgres://..." up
```

> `go build ./...` requires the fastText C headers (`fasttext.h`). `run.sh` downloads the model binary at startup.
> Unit tests in `storage/algorithms/` have no C dependency and can run without fastText installed.

## Architecture

### Data Flow

1. **Firehose** (`/firehose/`) subscribes to Bluesky Jetstream (4 US endpoints). Events (posts, follows, likes,
   reposts) are dispatched to a worker pool.
2. **Storage** (`/storage/storage.go`) is the central orchestrator: it writes to PostgreSQL (via sqlc), and caches
   users/posts/timelines in Redis.
3. **Algorithms** (`/storage/algorithms/`) implement feed filtering logic — each algorithm receives a post plus a
   snapshot of author statistics and returns whether the post should appear in that feed.
4. **Server** (`/server/`) exposes XRPC endpoints Bluesky expects from feed generators, plus `/healthz`, `/metrics`,
   and `/debug/`.
5. **Backfill** (`/backfill/`) optionally syncs historical repos from the PLS directory and PDS servers.
6. **Tasks** (`/tasks/`) runs background jobs: statistics updater (fetches follower counts from Bluesky PDS, sleeps
   1 minute between scans) and old data cleanup (runs hourly).

### Feed Read Path (Redis-only)

Feed reads hit **only Redis** — there is no database fallback. Each feed is a Redis sorted set
(`feed__<name>`) keyed by score. Rank is computed once at post creation as
`createdAt.Unix() + fnv32(uri)/10^digits` and written with `ZAddNX` (write-once). Reads use
`ZRevRangeByScore` (descending) with the cursor as the score boundary.

**Implication**: if Redis is empty or evicted, feeds serve empty lists with HTTP 200 and no error log.
Redis runs with `--maxmemory 4gb --maxmemory-policy allkeys-lru` to bound memory use.

### Key Packages

- `storage/algorithms/algorithm.go` — registers all feeds as entries in `ImplementedAlgorithms`. Adding a new feed
  means adding an entry here; the storage manager and server pick it up automatically.
- `storage/algorithms/top_languages.go` — `TopLanguageAlgorithm` admission filter: language, non-reply, follower
  floor, engagement factor, bot-ratio heuristic, account age, text length, uppercase ratio.
- `storage/cache/` — Redis caching for users (followers/follows/posts/interactions/createdAt), posts, and timeline
  sorted sets (3-day lifespan trimmed hourly).
- `storage/db/sqlc/` — Auto-generated type-safe DB code. Edit `storage/db/queries/*.sql` and run `sqlc generate`.
  Never edit `storage/db/sqlc/` directly.
- `utils/` — Language detection (fastText wrapper) and misc helpers (`IntFromString`, `Recoverer`, `SplitUri`).
- `monitoring/` — Prometheus middleware for HTTP and firehose metrics.

### Concurrency Model

- **Firehose worker pool** (`FIREHOSE_WORKER_POOL_SIZE`, default 100) and **storage worker pool**
  (`STORAGE_WORKER_POOL_SIZE`, default 100) are separate semaphore channels.
- The `pgxpool` is capped at 40 connections (`main.go`).
- All goroutines are wrapped in `utils.Recoverer` for panic recovery and restart.
- Author statistics are snapshotted once before goroutine dispatch — `AcceptsPost` sees the state at enqueue time.

### Feeds

Feeds are registered in `storage/algorithms/algorithm.go` (`ImplementedAlgorithms`). Current feeds: `basque`,
`catalan`, `galician`, `portuguese`, `spanish`, `chinese`, `top_spanish`, `top_portuguese`, `light_spanish`.

Feed URI format: `at://<BSKY_REPO>/app.bsky.feed.generator/<feed-name>`.

## Configuration

Copy `.env.example` to `.env`. Key variables:

| Variable                         | Purpose                                        |
|----------------------------------|------------------------------------------------|
| `DB_HOST/PORT/USERNAME/PASSWORD` | PostgreSQL connection                          |
| `REDIS_HOST/PORT`                | Redis connection                               |
| `BSKY_REPO`                      | DID of the feed generator account              |
| `BSKY_HOSTNAME`                  | Public domain of this service                  |
| `STATISTICS_USER/PASSWORD`       | Credentials for profile stats fetching         |
| `RUN_BACKFILL`                   | Set to `true` to enable historical backfill    |
| `FIREHOSE_WORKER_POOL_SIZE`      | Concurrency for event processing (default 100) |
| `BACKFILL_REPO_WORKERS`          | Concurrency for repo backfill (default 8)      |

### Top-feed tuning (no redeploy needed)

| Variable                        | Default | Effect                                       |
|---------------------------------|---------|----------------------------------------------|
| `TOP_FEED_MIN_FOLLOWERS`        | 1000    | Minimum author followers                     |
| `TOP_FEED_MIN_ENGAGEMENT`       | 1.0     | Minimum author engagement factor             |
| `TOP_FEED_MAX_FOLLOWS_RATIO`    | 5.0     | Max follows/followers ratio (bot heuristic)  |
| `TOP_FEED_MIN_ACCOUNT_AGE_DAYS` | 7       | Minimum account age in days (0 = unknown OK) |
| `TOP_FEED_MIN_TEXT_LENGTH`      | 20      | Minimum post text length in runes            |
| `TOP_FEED_MAX_UPPERCASE_RATIO`  | 0.7     | Max fraction of letters that are uppercase   |

## Database

Migrations live in `storage/db/migrations/`. The app applies migrations on startup via `run.sh`. Tables: `users`,
`posts`, `interactions`, `follows`, `subscription_state`.

`interactions` older than 7 days are deleted hourly in batches of 10,000 rows. `posts` cache entries (Redis) are
trimmed after 7 days, but the `posts` table itself is never pruned — deletes from the Bluesky firehose remove rows.

After editing SQL queries in `storage/db/queries/`, run `sqlc generate` to regenerate `storage/db/sqlc/`.
