package feeds

import (
	"bsky/storage/models"
	"encoding/json"
)

type Response struct {
	Cursor string
	Posts  []models.Post
}

func (r Response) MarshalJSON() ([]byte, error) {
	result := map[string]any{
		"cursor": r.Cursor,
	}

	posts := make([]any, len(r.Posts))
	for i, post := range r.Posts {
		serializedPost := map[string]any{
			"post": post.Uri,
		}
		if post.Reason != nil {
			serializedPost["reason"] = post.Reason
		}
		posts[i] = serializedPost
	}
	result["feed"] = posts

	return json.Marshal(result)
}
