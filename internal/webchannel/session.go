package webchannel

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
)

// Manager owns all active BrowserChannel sessions.
type Manager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

// NewManager creates an empty Manager.
func NewManager() *Manager {
	return &Manager{sessions: make(map[string]*Session)}
}

// NewSession creates a new Session, registers it, and returns it.
func (m *Manager) NewSession() *Session {
	id := newSessionID()
	s := &Session{
		ID:     id,
		mgr:    m,
		outbox: make(chan []byte, 128),
	}
	m.mu.Lock()
	m.sessions[id] = s
	m.mu.Unlock()
	return s
}

// Get returns the session with the given ID, or nil.
func (m *Manager) Get(id string) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions[id]
}

// Remove deletes the session from the registry.
func (m *Manager) Remove(id string) {
	m.mu.Lock()
	delete(m.sessions, id)
	m.mu.Unlock()
}

func newSessionID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Session holds the state for a single BrowserChannel connection.
type Session struct {
	ID     string
	mgr    *Manager
	seq    atomic.Int64 // next message sequence number
	outbox chan []byte  // raw BrowserChannel chunks to write to the GET backchannel
}

// Send enqueues a serialized BrowserChannel chunk for delivery to the client.
func (s *Session) Send(chunk []byte) {
	select {
	case s.outbox <- chunk:
	default: // drop if buffer full (slow client)
	}
}

// Outbox returns the output channel for the GET handler to drain.
func (s *Session) Outbox() <-chan []byte {
	return s.outbox
}

// EncodeGRPCWebFrame wraps proto bytes in a gRPC-Web frame:
// [0x00 (no compression)][4-byte big-endian length][proto bytes]
func EncodeGRPCWebFrame(proto []byte) []byte {
	frame := make([]byte, 5+len(proto))
	frame[0] = 0x00
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(proto)))
	copy(frame[5:], proto)
	return frame
}

// DecodeGRPCWebFrame extracts the proto bytes from a gRPC-Web frame.
// Returns the payload bytes, or an error if the frame is malformed.
func DecodeGRPCWebFrame(frame []byte) ([]byte, error) {
	if len(frame) < 5 {
		return nil, fmt.Errorf("webchannel: frame too short (%d bytes)", len(frame))
	}
	length := binary.BigEndian.Uint32(frame[1:5])
	if int(length) > len(frame)-5 {
		return nil, fmt.Errorf("webchannel: frame length %d exceeds data (%d bytes)", length, len(frame)-5)
	}
	return frame[5 : 5+length], nil
}

// FormatConnectChunk returns the BrowserChannel JSON chunk for session establishment.
// Format: <N>\n[[0,["c","<sessionId>","",8,8,0]],[1,["noop"]]]
func (s *Session) FormatConnectChunk() []byte {
	msgs := []interface{}{
		[]interface{}{0, []interface{}{"c", s.ID, "", 8, 8, 0}},
		[]interface{}{1, []interface{}{"noop"}},
	}
	s.seq.Store(2)
	b, _ := json.Marshal(msgs)
	return []byte(fmt.Sprintf("%d\n%s", len(b), b))
}

// FormatDataChunk wraps a gRPC-Web frame in a BrowserChannel JSON data chunk.
// frame must be a complete gRPC-Web frame (output of EncodeGRPCWebFrame).
func (s *Session) FormatDataChunk(frame []byte) []byte {
	seq := s.seq.Add(1) - 1
	encoded := base64.StdEncoding.EncodeToString(frame)
	msgs := []interface{}{
		[]interface{}{seq, []interface{}{encoded}},
	}
	b, _ := json.Marshal(msgs)
	return []byte(fmt.Sprintf("%d\n%s", len(b), b))
}

// FormatNoopChunk returns a keep-alive noop chunk.
func (s *Session) FormatNoopChunk() []byte {
	seq := s.seq.Add(1) - 1
	msgs := []interface{}{
		[]interface{}{seq, []interface{}{"noop"}},
	}
	b, _ := json.Marshal(msgs)
	return []byte(fmt.Sprintf("%d\n%s", len(b), b))
}
