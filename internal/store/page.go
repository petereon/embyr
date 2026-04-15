package store

import (
	"encoding/base64"
	"strconv"
)

// EncodePageToken encodes an integer offset as an opaque page token.
func EncodePageToken(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// DecodePageToken decodes a page token back to an offset.
// Returns 0 for an empty or invalid token.
func DecodePageToken(token string) int {
	if token == "" {
		return 0
	}
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0
	}
	n, err := strconv.Atoi(string(b))
	if err != nil || n < 0 {
		return 0
	}
	return n
}
