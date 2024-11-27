package backfill

type PlcLogAuditOperation struct {
	AlsoKnownAs []string `json:"alsoKnownAs"`
}

type PlcLogAuditEntry struct {
	Did       string               `json:"did"`
	Operation PlcLogAuditOperation `json:"operation"`
	CreatedAt string               `json:"createdAt"`
}
