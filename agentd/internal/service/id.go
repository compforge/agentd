package service

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

func newID(prefix string) string {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		panic(fmt.Sprintf("generate %s id: %v", prefix, err))
	}
	return prefix + "_" + hex.EncodeToString(bytes)
}
