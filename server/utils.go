package server

import (
	"bsky/utils"
	log "github.com/sirupsen/logrus"
	"net/http"
	"net/url"
)

func sendError(w http.ResponseWriter, errorCode int, message string) {
	log.Info(message)
	w.WriteHeader(errorCode)
	resp := map[string]string{
		"error": message,
	}
	jsonResp := utils.ToJson(resp)
	w.Write(jsonResp)
}

func getQueryItem(values url.Values, key string) *string {
	value := values.Get(key)
	if value == "" {
		return nil
	}
	return &value
}
