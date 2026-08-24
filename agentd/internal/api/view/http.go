package view

type HealthResponse struct {
	OK bool `json:"ok"`
}

type Error struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type ErrorResponse struct {
	Type  string `json:"type"`
	Error Error  `json:"error"`
}
