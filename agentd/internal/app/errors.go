package app

import (
	"errors"
)

var (
	ErrNoCapacity = errors.New("no worker capacity")
	ErrInvalid    = errors.New("invalid request")
)
