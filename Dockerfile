FROM golang:latest

ADD . /app
WORKDIR /app

RUN go mod download
RUN go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest

RUN curl -L https://packagecloud.io/golang-migrate/migrate/gpgkey | apt-key add -
RUN echo "deb https://packagecloud.io/golang-migrate/migrate/ubuntu/ $(lsb_release -sc) main" > /etc/apt/sources.list.d/migrate.list
RUN apt-get update
RUN apt-get install -y migrate
RUN migrate -database 'postgres://postgres:postgres@db:5432/bsky_feeds?sslmode=disable' -path=db/migrations up
