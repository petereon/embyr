package webchannel_test

import (
	"encoding/json"
	"testing"

	"github.com/petereon/firstyr/internal/webchannel"
	"github.com/stretchr/testify/require"
)

func TestSession_NewAndEncodeMessage(t *testing.T) {
	mgr := webchannel.NewManager()
	sess := mgr.NewSession()
	require.NotEmpty(t, sess.ID)

	// Encode a trivial JSON proto payload (sendRawJson:true format)
	data := json.RawMessage(`{"targetChange":{"targetChangeType":"NO_CHANGE"}}`)
	msg := sess.FormatDataChunk(data)
	require.NotEmpty(t, msg)
	// verify it's valid JSON with BrowserChannel format: <len>\n<json>
	require.Contains(t, string(msg), "\n")
	require.Contains(t, string(msg), "targetChange")

	// Verify FormatConnectChunk contains the session ID
	connectMsg := sess.FormatConnectChunk()
	require.Contains(t, string(connectMsg), sess.ID)
}

func TestDecodeGRPCWebFrame(t *testing.T) {
	tests := []struct {
		name      string
		frame     []byte
		expectErr bool
		expectPayload []byte
	}{
		{
			name:      "valid frame round-trip",
			frame:     webchannel.EncodeGRPCWebFrame([]byte("hello world")),
			expectErr: false,
			expectPayload: []byte("hello world"),
		},
		{
			name:      "empty payload",
			frame:     webchannel.EncodeGRPCWebFrame([]byte{}),
			expectErr: false,
			expectPayload: []byte{},
		},
		{
			name:      "frame too short",
			frame:     []byte{0x00, 0x00, 0x00},
			expectErr: true,
		},
		{
			name:      "declared length exceeds available data",
			frame:     []byte{0x00, 0x00, 0x00, 0x00, 0x10, 0x01, 0x02}, // claims 16 bytes but only 2 bytes available
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Decode the frame
			decoded, err := webchannel.DecodeGRPCWebFrame(tt.frame)

			if tt.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.expectPayload, decoded)
		})
	}
}

func TestFormatNoopChunk(t *testing.T) {
	mgr := webchannel.NewManager()
	sess := mgr.NewSession()

	result := sess.FormatNoopChunk()

	// Assert non-empty
	require.NotEmpty(t, result)

	// Assert contains newline (BrowserChannel format separator)
	require.Contains(t, string(result), "\n")

	// Assert contains "noop"
	require.Contains(t, string(result), "noop")
}

func TestManager_GetAndRemove(t *testing.T) {
	mgr := webchannel.NewManager()

	// Create a session
	sess := mgr.NewSession()

	// Verify Get returns the session
	retrieved := mgr.Get(sess.ID)
	require.NotNil(t, retrieved)
	require.Equal(t, sess.ID, retrieved.ID)

	// Remove the session
	mgr.Remove(sess.ID)

	// Verify Get returns nil after removal
	retrieved = mgr.Get(sess.ID)
	require.Nil(t, retrieved)
}
