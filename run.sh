#! /bin/bash

# Apply pending migrations
migrate -database 'postgres://postgres:postgres@db:5432/bsky_feeds?sslmode=disable' -path=storage/db/migrations up

# Start server
go run .
