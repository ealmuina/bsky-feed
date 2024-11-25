package utils

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"runtime/debug"
	"strconv"
	"strings"
)

func IntFromString(s string, defaultValue int) int {
	atoi, err := strconv.Atoi(s)
	if err != nil {
		return defaultValue
	}
	return atoi
}

func ToJson(value any) []byte {
	jsonResp, err := json.Marshal(value)
	if err != nil {
		log.Errorf("Error happened in JSON marshal. Err: %s", err)
	}
	return jsonResp
}

func Recoverer(maxPanics, id int, f func()) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("HERE %v: %v", id, err)
			log.Errorf("stacktrace from panic: \n" + string(debug.Stack()))
			if maxPanics == 0 {
				panic("TOO MANY PANICS")
			} else {
				go Recoverer(maxPanics-1, id, f)
			}
		}
	}()
	f()
}

func SplitUri(uri string, category string) (authorDid string, uriKey string, err error) {
	parts := strings.Split(
		strings.TrimPrefix(uri, "at://"),
		category,
	)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid uri: %s", uri)
	}
	return parts[0], parts[1], nil
}
