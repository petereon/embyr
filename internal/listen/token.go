package listen

import (
	"encoding/base64"
	"fmt"
	"time"
)

// EncodeResumeToken encodes a timestamp as an opaque resume token (base64 RFC3339Nano).
func EncodeResumeToken(ts time.Time) []byte {
	s := ts.UTC().Format(time.RFC3339Nano)
	return []byte(base64.RawURLEncoding.EncodeToString([]byte(s)))
}

// DecodeResumeToken decodes a resume token back to a time.Time.
// Returns an error if the token is malformed.
func DecodeResumeToken(tok []byte) (time.Time, error) {
	b, err := base64.RawURLEncoding.DecodeString(string(tok))
	if err != nil {
		return time.Time{}, fmt.Errorf("listen: decode resume token: %w", err)
	}
	ts, err := time.Parse(time.RFC3339Nano, string(b))
	if err != nil {
		return time.Time{}, fmt.Errorf("listen: parse resume token timestamp: %w", err)
	}
	return ts, nil
}
