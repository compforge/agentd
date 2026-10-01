package service

import (
	"errors"
)

var (
	ErrClusterNoCapacity   = errors.New("cluster has no Worker capacity")
	ErrWorkerUnschedulable = errors.New("Worker scheduling is blocked")
	ErrNoCapacity          = errors.New("no worker capacity")
	ErrNoAssignment        = errors.New("session has no assignment")
	ErrUnavailable         = errors.New("assigned worker is unavailable")
	ErrInvalid             = errors.New("invalid request")
	ErrConflict            = errors.New("resource conflict")
	ErrUnsupported         = errors.New("unsupported feature")
)
