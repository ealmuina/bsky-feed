package models

import (
	"encoding/json"
	"time"
)

type Post struct {
	// Serialized
	Uri    string
	Reason map[string]string

	// Non serialized
	Rank        float64
	CreatedAt   time.Time
	AuthorDid   string
	ReplyParent string
	ReplyRoot   string
	Language    string
}

func (p *Post) MarshalJSON() ([]byte, error) {
	result := map[string]any{
		"post": p.Uri,
	}
	if p.Reason != nil {
		result["reason"] = p.Reason
	}
	return json.Marshal(result)
}

func (p *Post) UnmarshalJSON(b []byte) error {
	dict := map[string]any{}
	if err := json.Unmarshal(b, &dict); err != nil {
		return err
	}

	p.Uri = dict["post"].(string)
	p.Reason = dict["reason"].(map[string]string)

	return nil
}
