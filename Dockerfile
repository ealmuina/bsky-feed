FROM golang:latest

ADD . /app
WORKDIR /app

RUN go mod download

#RUN go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
RUN go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
