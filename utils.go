package wecomaibot

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

func generateRandomString(length int) string {
	if length <= 0 {
		length = 8
	}
	byteLen := (length + 1) / 2
	b := make([]byte, byteLen)
	if _, err := rand.Read(b); err != nil {
		now := time.Now().UnixNano()
		return fmt.Sprintf("%x", now)[:length]
	}
	s := hex.EncodeToString(b)
	if len(s) > length {
		return s[:length]
	}
	return s
}

func GenerateReqID(prefix string) string {
	return fmt.Sprintf("%s_%d_%s", prefix, time.Now().UnixMilli(), generateRandomString(8))
}
