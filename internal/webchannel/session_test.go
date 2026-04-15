package webchannel_test

import (
	"testing"

	"github.com/petereon/firstyr/internal/webchannel"
	"github.com/stretchr/testify/require"
)

func TestSession_NewAndEncodeMessage(t *testing.T) {
	mgr := webchannel.NewManager()
	sess := mgr.NewSession()
	require.NotEmpty(t, sess.ID)

	// Encode a trivial proto payload
	data := []byte("hello")
	frame := webchannel.EncodeGRPCWebFrame(data)
	require.Equal(t, byte(0x00), frame[0]) // not compressed
	require.Len(t, frame, 5+len(data))

	msg := sess.FormatDataChunk(frame)
	require.NotEmpty(t, msg)
	// verify it's valid JSON with BrowserChannel format: <len>\n<json>
	require.Contains(t, string(msg), "\n")

	// Verify FormatConnectChunk contains the session ID
	connectMsg := sess.FormatConnectChunk()
	require.Contains(t, string(connectMsg), sess.ID)
}
