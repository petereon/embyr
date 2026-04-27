package listen_test

import (
	"testing"
	"time"

	"github.com/petereon/embyr/internal/listen"
	"github.com/stretchr/testify/require"
)

func TestResumeToken_RoundTrip(t *testing.T) {
	ts := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	tok := listen.EncodeResumeToken(ts)
	require.NotEmpty(t, tok)
	got, err := listen.DecodeResumeToken(tok)
	require.NoError(t, err)
	require.True(t, ts.Equal(got))
}

func TestResumeToken_InvalidInput(t *testing.T) {
	_, err := listen.DecodeResumeToken([]byte("not-base64!!!"))
	require.Error(t, err)
}
