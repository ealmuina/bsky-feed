FROM golang:latest

ADD . /app
WORKDIR /app

RUN go mod download
RUN go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest

RUN curl -L https://github.com/golang-migrate/migrate/releases/download/$version/migrate.$os-$arch.tar.gz | tar xvz
RUN migrate -database 'postgres://postgres:postgres@db:5432/bsky_feeds?sslmode=disable' -path=db/migrations up
