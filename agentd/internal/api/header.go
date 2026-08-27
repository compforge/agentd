package api

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	requestIDHeader           = "request-id"
	stainlessRetryCountHeader = "X-Stainless-Retry-Count"
)

func parseStainlessRetryCount(raw []byte) (int, error) {
	value := strings.TrimSpace(string(raw))
	if value == "" {
		return 0, nil
	}
	retryCount, err := strconv.Atoi(value)
	if err != nil || retryCount < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", stainlessRetryCountHeader)
	}
	return retryCount, nil
}
