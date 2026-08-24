package api

import (
	"fmt"

	hertzapp "github.com/cloudwego/hertz/pkg/app"
	"github.com/compforge/agentd/agentd/internal/service"
)

func bindRequest(request *hertzapp.RequestContext, target any) error {
	if err := request.BindAndValidate(target); err != nil {
		return fmt.Errorf("%w: %v", service.ErrInvalid, err)
	}
	return nil
}
