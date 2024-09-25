#! /bin/bash

# Download fasttext language detection model
if ! [ -e "/app/utils/lid.176.bin" ] ; then
    curl -o /app/utils/lid.176.bin https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin
fi

# Apply pending migrations
migrate -database 'postgres://postgres:postgres@db:5432/bsky_feeds?sslmode=disable' -path=storage/db/migrations up

# Start server
go run .
