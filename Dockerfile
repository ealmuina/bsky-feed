FROM golang:latest

ADD . /app
WORKDIR /app

RUN go mod download

RUN go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
RUN migrate -database 'postgres://postgres:postgres@db:5432/bsky_feeds?sslmode=disable' -path=db/migrations up
