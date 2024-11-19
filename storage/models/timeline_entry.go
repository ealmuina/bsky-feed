package models

type TimelineEntry struct {
	Uri    string
	Reason map[string]string
	Rank   float64
}
