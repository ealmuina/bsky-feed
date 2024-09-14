package utils

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"strconv"
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
			if maxPanics == 0 {
				panic("TOO MANY PANICS")
			} else {
				go Recoverer(maxPanics-1, id, f)
			}
		}
	}()
	f()
}
