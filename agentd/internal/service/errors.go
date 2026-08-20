package service

import (
	"errors"
)

var (
	ErrNoCapacity   = errors.New("no worker capacity")
	ErrNoAssignment = errors.New("session has no assignment")
	ErrUnavailable  = errors.New("assigned worker is unavailable")
	ErrInvalid      = errors.New("invalid request")
	ErrConflict     = errors.New("resource conflict")
	ErrUnsupported  = errors.New("unsupported feature")
)
