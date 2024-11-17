package feeds

import (
	"bsky/storage/cache"
	"encoding/json"
)

type Response struct {
	Cursor  string
	Entries []cache.TimelineEntry
}

func (r Response) MarshalJSON() ([]byte, error) {
	result := map[string]any{
		"cursor": r.Cursor,
	}

	entries := make([]any, len(r.Entries))
	for i, entry := range r.Entries {
		serializedPost := map[string]any{
			"post": entry.Post.Uri,
		}
		if entry.Reason != nil {
			serializedPost["reason"] = entry.Reason
		}
		entries[i] = serializedPost
	}
	result["feed"] = entries

	return json.Marshal(result)
}
