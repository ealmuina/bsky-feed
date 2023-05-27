package utils

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"strconv"
	"time"
)

func CleanOldData(db *gorm.DB, tables []any) {
	for {
		select {
		case <-time.After(1 * time.Hour):
			now := time.Now()
			for _, table := range tables {
				db.Unscoped().Delete(
					table,
					"created_at < ?",
					now.Add(-48*time.Hour),
				)
			}
		}
	}
}

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
