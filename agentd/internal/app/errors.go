package app

import (
	"errors"
)

var (
	ErrNoCapacity  = errors.New("no worker capacity")
	ErrInvalid     = errors.New("invalid request")
	ErrConflict    = errors.New("resource conflict")
	ErrUnsupported = errors.New("unsupported feature")
)
