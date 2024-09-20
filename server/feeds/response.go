package feeds

import "bsky/storage/models"

type Response struct {
	Cursor string        `json:"cursor"`
	Posts  []models.Post `json:"feed"`
}
