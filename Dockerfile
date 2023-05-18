FROM golang:latest

ADD . /app
WORKDIR /app

RUN go mod download
