package algorithms

import (
	"bsky/storage/cache"
	db "bsky/storage/db/sqlc"
	"bsky/storage/models"
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	"os"
)

type AiAlgorithm struct {
	languageCode string
	prompt       string
}

func (a *AiAlgorithm) aiAccepted(text string) bool {
	aiUrl := os.Getenv("AI_URL")
	aiUser := os.Getenv("AI_USER")
	aiPassword := os.Getenv("AI_PASSWORD")

	prompt := fmt.Sprintf(
		"Answer '1' if true or '0' if false, without any further explanation.\n%s: \"%s\"?",
		a.prompt,
		text,
	)
	payload, err := json.Marshal(map[string]any{
		"model":  "phi3.5",
		"prompt": prompt,
		"stream": false,
		"options": map[string]any{
			"seed": 2024,
		},
	})
	if err != nil {
		log.Errorf("Error serializing payload for AI: %v", err)
		return false
	}

	request, err := http.NewRequest(
		"POST",
		aiUrl,
		bytes.NewReader(payload),
	)
	if err != nil {
		log.Errorf("Error creating request for AI: %v", err)
		return false
	}
	request.SetBasicAuth(aiUser, aiPassword)
	respose, err := http.DefaultClient.Do(request)
	if err != nil {
		log.Errorf("Error executing request for AI: %v", err)
		return false
	}
	defer respose.Body.Close()

	bodyBytes, err := io.ReadAll(respose.Body)
	if err != nil {
		log.Errorf("Error reading response for AI: %v", err)
	}
	bodyString := string(bodyBytes)

	if respose.StatusCode != http.StatusOK {
		log.Errorf("Error executing request for AI. Status: %v. Content: %v", respose.Status, bodyString)
		return false
	}

	bodyMap := make(map[string]any)
	err = json.Unmarshal(bodyBytes, &bodyMap)
	if err != nil {
		log.Errorf("Error unmarshalling response for AI: %v", err)
	}

	return bodyMap["response"].(string) == "1"
}

func (a *AiAlgorithm) AcceptsPost(post models.Post, _ cache.UserStatistics) (ok bool, reason map[string]string) {
	ok = post.Language == a.languageCode &&
		post.ReplyRoot == "" &&
		a.aiAccepted(post.Text)
	reason = nil
	return
}

func (a *AiAlgorithm) GetPosts(_ *db.Queries, _ float64, _ int64) []models.Post {
	// These timelines are stored in memory only
	return make([]models.Post, 0)
}
