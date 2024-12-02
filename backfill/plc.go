package backfill

type PlcLogEntry struct {
	Did       string          `json:"did"`
	Operation PlcLogOperation `json:"operation"`
	CreatedAt string          `json:"createdAt"`
}

type PlcLogOperation struct {
	AlsoKnownAs []string        `json:"alsoKnownAs"`
	Services    *PlcLogServices `json:"services,omitempty"`
}

type PlcLogServices struct {
	AtProtoPds PlcLogAtProtoPds `json:"atproto_pds"`
}

type PlcLogAtProtoPds struct {
	Endpoint string `json:"endpoint"`
}
