package webchannel

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
)

// MsgLog is a append-only message log with fan-out notification.
// Pump goroutines append; GET back-channel handlers read and replay.
type MsgLog struct {
	mu      sync.Mutex
	entries [][]byte
	ready   chan struct{} // closed when new data arrives; replaced each time
}

func newMsgLog() *MsgLog {
	return &MsgLog{ready: make(chan struct{})}
}

// Append stores chunk and wakes up all waiting readers.
func (ml *MsgLog) Append(chunk []byte) {
	ml.mu.Lock()
	ml.entries = append(ml.entries, chunk)
	ch := ml.ready
	ml.ready = make(chan struct{})
	ml.mu.Unlock()
	close(ch)
}

// From returns all log entries starting at idx, and a channel that is closed
// when the next entry arrives. The caller should advance its index by
// len(returned) and loop.
func (ml *MsgLog) From(idx int) ([][]byte, <-chan struct{}) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if idx < 0 {
		idx = 0
	}
	var out [][]byte
	if idx < len(ml.entries) {
		out = make([][]byte, len(ml.entries)-idx)
		copy(out, ml.entries[idx:])
	}
	return out, ml.ready
}

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
		ID:        id,
		mgr:       m,
		listenLog: newMsgLog(),
		writeLog:  newMsgLog(),
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
	ID  string
	mgr *Manager
	seq atomic.Int64 // next message sequence number
	// message logs for fan-out replay to concurrent GET back-channels
	listenLog *MsgLog
	writeLog  *MsgLog
	// set after session establishment by the HTTP handler; exactly one of these is non-nil
	bridge      *listenBridge
	writeBridge *writeBridge
	cancel      context.CancelFunc
	// RID deduplication: BrowserChannel retries POSTs with same RID on timeout
	lastRIDMu sync.Mutex
	lastRID   string
}

// SeenRID returns true if rid was already processed, false and records it otherwise.
func (s *Session) SeenRID(rid string) bool {
	s.lastRIDMu.Lock()
	defer s.lastRIDMu.Unlock()
	if s.lastRID == rid {
		return true
	}
	s.lastRID = rid
	return false
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

// FormatDataChunk wraps a JSON proto message in a BrowserChannel data chunk.
// data must be a valid JSON value (the Firestore proto in REST/proto3-JSON format).
// WebChannel is configured with sendRawJson:true, so messages are plain JSON objects,
// not base64-encoded gRPC-Web binary frames.
func (s *Session) FormatDataChunk(data json.RawMessage) []byte {
	seq := s.seq.Add(1) - 1
	msgs := []interface{}{
		[]interface{}{seq, []interface{}{data}},
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
