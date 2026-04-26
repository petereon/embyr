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

	result := sess.FormatNoopChunk(42)

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

// TestSession_NoopDoesNotAdvanceGlobalSeq verifies that FormatNoopChunk does
// NOT advance the session-global seq counter. Noops are per-connection
// keepalives; advancing the global counter breaks the logIdx = AID-1 mapping
// (data chunk at log[0] must always have seq=2, log[1]→seq=3, etc.).
func TestSession_NoopDoesNotAdvanceGlobalSeq(t *testing.T) {
	mgr := webchannel.NewManager()
	sess := mgr.NewSession()
	sess.FormatConnectChunk() // initialises seq to 2

	data1 := sess.FormatDataChunk(json.RawMessage(`{}`))
	require.Contains(t, string(data1), `[2,`) // first data chunk must have seq=2

	// Noops must NOT advance the global seq.
	sess.FormatNoopChunk(2)
	sess.FormatNoopChunk(2)

	data2 := sess.FormatDataChunk(json.RawMessage(`{}`))
	require.Contains(t, string(data2), `[3,`) // second data chunk must have seq=3, not 5
}

func TestSession_SeenRID_RetryDetection(t *testing.T) {
	mgr := webchannel.NewManager()
	sess := mgr.NewSession()

	require.False(t, sess.SeenRID("2"), "first time RID=2 must not be seen")
	require.False(t, sess.SeenRID("3"), "first time RID=3 must not be seen")
	// Retry of a previous RID must be detected even after a newer RID was processed.
	require.True(t, sess.SeenRID("2"), "retry of RID=2 after RID=3 must be detected")
}
