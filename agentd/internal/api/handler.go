package api

import (
	"context"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
)

type apiHandler func(context.Context, *hertzapp.RequestContext) error

func adaptHandler(next apiHandler) hertzapp.HandlerFunc {
	return func(ctx context.Context, request *hertzapp.RequestContext) {
		if err := next(ctx, request); err != nil {
			_ = request.Error(err)
		}
	}
}
