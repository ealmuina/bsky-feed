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

# Run tests
go test ./...

# Run a single test
go test ./tests/... -run TestName

# Regenerate SQLC query code (after editing /storage/db/queries/*.sql)
sqlc generate

# Apply DB migrations manually
migrate -path storage/db/migrations -database "postgres://..." up
```

## Architecture

### Data Flow

1. **Firehose** (`/firehose/`) subscribes to Bluesky Jetstream (4 US endpoints). Events (posts, follows, likes, reposts)
   are dispatched to a worker pool.
2. **Storage** (`/storage/storage.go`) is the central orchestrator: it writes to PostgreSQL (via sqlc), and caches
   users/posts/timelines in Redis.
3. **Algorithms** (`/storage/algorithms/`) implement feed filtering logic — each algorithm receives posts and returns
   filtered/ranked results.
4. **Server** (`/server/`) exposes three XRPC endpoints Bluesky expects from feed generators, plus `/metrics` and
   `/debug/`.
5. **Backfill** (`/backfill/`) optionally syncs historical repos from the PLS directory and PDS servers.
6. **Tasks** (`/tasks/`) runs background jobs: statistics updater (fetches follower counts from Bluesky PDS) and old
   data cleanup.

### Key Packages

- `storage/algorithms/` — Feed algorithm implementations. `LanguageAlgorithm` does simple language filtering;
  `TopLanguageAlgorithm` adds engagement scoring with a minimum follower threshold.
- `storage/cache/` — Redis caching for users, posts, and timeline windows (3-day lifespan).
- `storage/db/sqlc/` — Auto-generated type-safe DB code. Edit `storage/db/queries/*.sql` and run `sqlc generate`, never
  edit sqlc output directly.
- `utils/` — Language detection (fastText wrapper) and misc helpers.
- `monitoring/` — Prometheus middleware for HTTP and firehose metrics.

### Feeds

Each feed is registered in `storage/storage.go` and mapped to an algorithm. The feed URI format is
`at://<BSKY_REPO>/app.bsky.feed.generator/<feed-name>`. Current feeds: `basque`, `catalan`, `galician`, `portuguese`,
`spanish`, `chinese`, `top_spanish`, `top_portuguese`, `light_spanish`.

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

FastText model (`lid.176.bin`) is downloaded automatically by `run.sh` at startup. Set `CPLUS_INCLUDE_PATH` if fastText
headers aren't in the default location.

## Database

Migrations live in `storage/db/migrations/`. The app uses `golang-migrate` and applies migrations on startup via
`run.sh`. Tables: `users`, `posts`, `interactions`, `follows`, `subscription_state`.

After editing SQL queries in `storage/db/queries/`, run `sqlc generate` to regenerate `storage/db/sqlc/`.
