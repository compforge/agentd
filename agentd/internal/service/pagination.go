package service

import "github.com/compforge/agentd/agentd/internal/repo"

type PageQuery struct {
	Offset     int
	Limit      int
	Descending bool
}

type Page[T any] struct {
	Items   []T
	HasMore bool
}

func repositoryPageQuery(query PageQuery) repo.PageQuery {
	return repo.PageQuery{
		Offset: query.Offset, Limit: query.Limit, Descending: query.Descending,
	}
}

func servicePage[T any](page repo.Page[T]) Page[T] {
	return Page[T]{Items: page.Items, HasMore: page.HasMore}
}
