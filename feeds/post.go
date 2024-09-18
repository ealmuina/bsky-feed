package feeds

import (
	"encoding/json"
	"time"
)

type Post struct {
	// Serialized
	Uri       string
	Reason    map[string]string
	CreatedAt time.Time
	Cid       string

	// Non serialized
	AuthorDid   string
	ReplyParent string
	ReplyRoot   string
	Language    string
}

func (p *Post) MarshalJSON() ([]byte, error) {
	result := map[string]any{
		"post":       p.Uri,
		"cid":        p.Cid,
		"created_at": p.CreatedAt.Unix(),
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
	p.CreatedAt = time.Unix(dict["created_at"].(int64), 0)
	p.Cid = dict["cid"].(string)

	return nil
}
