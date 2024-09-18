package feeds

type Response struct {
	Cursor string `json:"cursor"`
	Posts  []Post `json:"feeds"`
}
