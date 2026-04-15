package store_test

import (
	"encoding/base64"
	"testing"

	"github.com/petereon/firstyr/internal/store"
	"github.com/stretchr/testify/assert"
)

func TestPageToken_RoundTrip(t *testing.T) {
	for _, offset := range []int{0, 1, 50, 100, 999} {
		token := store.EncodePageToken(offset)
		assert.NotEmpty(t, token)
		decoded := store.DecodePageToken(token)
		assert.Equal(t, offset, decoded, "offset %d should round-trip", offset)
	}
}

func TestPageToken_EmptyToken(t *testing.T) {
	assert.Equal(t, 0, store.DecodePageToken(""))
}

func TestPageToken_CorruptedBase64(t *testing.T) {
	assert.Equal(t, 0, store.DecodePageToken("not-valid-base64!!!"))
}

func TestPageToken_NegativeEncoded(t *testing.T) {
	// Manually encode a negative number and verify the guard returns 0
	tok := base64.RawURLEncoding.EncodeToString([]byte("-5"))
	assert.Equal(t, 0, store.DecodePageToken(tok))
}
