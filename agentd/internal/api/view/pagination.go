package view

type PageRequest struct {
	Limit int    `query:"limit"`
	Page  string `query:"page"`
}

type Page[T any] struct {
	Data     []T     `json:"data"`
	NextPage *string `json:"next_page"`
}

type BidirectionalPage[T any] struct {
	Data     []T     `json:"data"`
	NextPage *string `json:"next_page"`
	PrevPage *string `json:"prev_page"`
}
