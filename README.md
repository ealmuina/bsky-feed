# Bluesky Feed Generator

## Useful commands

### Generate SQLC queries

```
sqlc generate
```

### Apply pending migrations

```
migrate -database 'postgres://postgres:postgres@db:5432/bsky_feeds?sslmode=disable' -path=db/migrations up
```

### Generate new migration

```
migrate create -ext sql -dir db/migrations <MigrationName>
```
