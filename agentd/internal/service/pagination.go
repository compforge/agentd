package service

import (
	"time"

	"github.com/compforge/agentd/agentd/internal/repo"
)

type PageQuery struct {
	Limit      int
	Descending bool
	Direction  PageDirection
	Anchor     *PageAnchor
}

type PageDirection string

const (
	PageAfter  PageDirection = "after"
	PageBefore PageDirection = "before"
)

type PageAnchor struct {
	CreatedAt time.Time
	ID        string
	Version   int64
}

type Page[T any] struct {
	Items   []T
	HasMore bool
}

func repositoryPageQuery(query PageQuery) repo.PageQuery {
	page := repo.PageQuery{
		Limit: query.Limit, Descending: query.Descending, Direction: repo.PageDirection(query.Direction),
	}
	if query.Anchor != nil {
		page.Anchor = &repo.PageAnchor{
			CreatedAt: query.Anchor.CreatedAt, ID: query.Anchor.ID, Version: query.Anchor.Version,
		}
	}
	return page
}

func servicePage[T any](page repo.Page[T]) Page[T] {
	return Page[T]{Items: page.Items, HasMore: page.HasMore}
}
