# Bluesky Feed Generator

## Useful commands

### Generate SQLC queries

```bash
sqlc generate
```

### Apply pending migrations

```bash
migrate -database 'postgres://postgres:postgres@db:5432/bsky_feeds?sslmode=disable' -path=storage/db/migrations up
```

### Generate new migration

```bash
migrate create -ext sql -dir storage/db/migrations <MigrationName>
```

### Create DB Dump

```bash
DATE=$(date +%Y%m%d-%H%M%S)
docker compose exec -it db pg_dumpall -U postgres | gzip -9c > db-$DATE.sql.gz
```

### Load DB Dump

```bash
gzip -d <db_dump>
cat <db_dump.sql> | docker compose exec -T db psql -U postgres -d bsky_feeds
```