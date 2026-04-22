package webchannel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
)

// listenBridge is a fake Firestore_ListenServer that routes between HTTP and the gRPC handler.
type listenBridge struct {
	ctx    context.Context
	sendCh chan *firestorev1.ListenResponse
	recvCh chan *firestorev1.ListenRequest
}

func newListenBridge(ctx context.Context) *listenBridge {
	return &listenBridge{
		ctx:    ctx,
		sendCh: make(chan *firestorev1.ListenResponse, 64),
		recvCh: make(chan *firestorev1.ListenRequest, 8),
	}
}

// grpc.ServerStream interface
func (b *listenBridge) SetHeader(metadata.MD) error  { return nil }
func (b *listenBridge) SendHeader(metadata.MD) error { return nil }
func (b *listenBridge) SetTrailer(metadata.MD)        {}
func (b *listenBridge) Context() context.Context      { return b.ctx }
func (b *listenBridge) SendMsg(m any) error           { return nil }
func (b *listenBridge) RecvMsg(m any) error           { return nil }

// Firestore_ListenServer interface
func (b *listenBridge) Send(resp *firestorev1.ListenResponse) error {
	select {
	case b.sendCh <- resp:
		return nil
	case <-b.ctx.Done():
		return b.ctx.Err()
	}
}

func (b *listenBridge) Recv() (*firestorev1.ListenRequest, error) {
	select {
	case req := <-b.recvCh:
		return req, nil
	case <-b.ctx.Done():
		return nil, io.EOF
	}
}

// Handler handles both POST (forward channel) and GET (back channel) for BrowserChannel.
type Handler struct {
	mgr      *Manager
	listenFn func(firestorev1.Firestore_ListenServer) error
}

// NewHandler creates a BrowserChannel Handler.
func NewHandler(mgr *Manager, listenFn func(firestorev1.Firestore_ListenServer) error) *Handler {
	return &Handler{mgr: mgr, listenFn: listenFn}
}

// ServeHTTP dispatches to the POST (forward) or GET (back) channel handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.handleForward(w, r)
	case http.MethodGet:
		h.handleBack(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleForward handles POST /Listen/channel.
func (h *Handler) handleForward(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	sid := q.Get("SID")
	rid := q.Get("RID")

	if sid == "" && rid != "" {
		// New session establishment.
		sess := h.mgr.NewSession()

		reqs, _ := parseForwardBody(r)

		ctx, cancel := context.WithCancel(context.Background())
		bridge := newListenBridge(ctx)

		sess.bridge = bridge
		sess.cancel = cancel

		// gRPC handler goroutine.
		go func() {
			defer cancel()
			defer h.mgr.Remove(sess.ID)
			_ = h.listenFn(bridge)
		}()

		// Pump goroutine: reads ListenResponses from the gRPC handler and
		// appends BrowserChannel JSON chunks to the session log for replay to any
		// number of concurrent GET back-channel handlers.
		// WebChannel uses sendRawJson:true so messages are proto3-JSON objects.
		go func() {
			for {
				select {
				case resp, ok := <-bridge.sendCh:
					if !ok {
						return
					}
					b, err := protojson.Marshal(resp)
					if err != nil {
						continue
					}
					chunk := sess.FormatDataChunk(json.RawMessage(b))
					sess.listenLog.Append(chunk)
				case <-bridge.ctx.Done():
					return
				}
			}
		}()

		log.Printf("[webchannel] POST new-session sid=%s parsed=%d reqs", sess.ID, len(reqs))
		for _, req := range reqs {
			select {
			case bridge.recvCh <- req:
			default:
			}
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(sess.FormatConnectChunk())
		return
	}

	// Forward channel message for existing session.
	sess := h.mgr.Get(sid)
	if sess == nil {
		http.Error(w, "session not found", http.StatusBadRequest)
		return
	}
	if !sess.SeenRID(rid) {
		reqs, _ := parseForwardBody(r)
		for _, req := range reqs {
			select {
			case sess.bridge.recvCh <- req:
			default:
			}
		}
	}
	w.WriteHeader(http.StatusOK)
}

// handleBack handles GET /Listen/channel — the long-polling back channel.
// Multiple concurrent connections (CI=0, CI=1, …) are each served independently
// from the session's message log, so no message is split between connections.
func (h *Handler) handleBack(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("SID")
	sess := h.mgr.Get(sid)
	if sess == nil {
		http.Error(w, "session not found", http.StatusBadRequest)
		return
	}

	// AID is the last array-id the client acknowledged. The first data chunk
	// is seq=2 (log index 0). For seq S, log index = S-2. We want seq > AID,
	// i.e., log index >= AID-1.
	logIdx := 0
	if aidStr := r.URL.Query().Get("AID"); aidStr != "" {
		if n, err := strconv.Atoi(aidStr); err == nil && n >= 2 {
			logIdx = n - 1
		}
	}
	ci := r.URL.Query().Get("CI")
	log.Printf("[webchannel] GET back-channel sid=%s CI=%s logIdx=%d", sid, ci, logIdx)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	keepAlive := time.NewTicker(25 * time.Second)
	defer keepAlive.Stop()

	for {
		chunks, notify := sess.listenLog.From(logIdx)
		for _, chunk := range chunks {
			_, _ = w.Write(chunk)
			flusher.Flush()
		}
		logIdx += len(chunks)

		select {
		case <-r.Context().Done():
			return
		case <-sess.bridge.ctx.Done():
			// Drain any remaining chunks before closing.
			final, _ := sess.listenLog.From(logIdx)
			for _, c := range final {
				_, _ = w.Write(c)
				flusher.Flush()
			}
			return
		case <-notify:
			// New data in log — loop to drain it.
		case <-keepAlive.C:
			_, _ = w.Write(sess.FormatNoopChunk())
			flusher.Flush()
		}
	}
}

// ── Write BrowserChannel ────────────────────────────────────────────────────

// writeBridge is a fake Firestore_WriteServer that routes between HTTP and the gRPC handler.
type writeBridge struct {
	ctx    context.Context
	sendCh chan *firestorev1.WriteResponse
	recvCh chan *firestorev1.WriteRequest
}

func newWriteBridge(ctx context.Context) *writeBridge {
	return &writeBridge{
		ctx:    ctx,
		sendCh: make(chan *firestorev1.WriteResponse, 64),
		recvCh: make(chan *firestorev1.WriteRequest, 8),
	}
}

func (b *writeBridge) SetHeader(metadata.MD) error  { return nil }
func (b *writeBridge) SendHeader(metadata.MD) error { return nil }
func (b *writeBridge) SetTrailer(metadata.MD)        {}
func (b *writeBridge) Context() context.Context      { return b.ctx }
func (b *writeBridge) SendMsg(m any) error            { return nil }
func (b *writeBridge) RecvMsg(m any) error            { return nil }

func (b *writeBridge) Send(resp *firestorev1.WriteResponse) error {
	select {
	case b.sendCh <- resp:
		return nil
	case <-b.ctx.Done():
		return b.ctx.Err()
	}
}

func (b *writeBridge) Recv() (*firestorev1.WriteRequest, error) {
	select {
	case req := <-b.recvCh:
		return req, nil
	case <-b.ctx.Done():
		return nil, io.EOF
	}
}

// WriteHandler handles the Write BrowserChannel (POST forward + GET back).
type WriteHandler struct {
	mgr     *Manager
	writeFn func(firestorev1.Firestore_WriteServer) error
}

// NewWriteHandler creates a WriteHandler.
func NewWriteHandler(mgr *Manager, writeFn func(firestorev1.Firestore_WriteServer) error) *WriteHandler {
	return &WriteHandler{mgr: mgr, writeFn: writeFn}
}

func (h *WriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.handleForward(w, r)
	case http.MethodGet:
		h.handleBack(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *WriteHandler) handleForward(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	sid := q.Get("SID")
	rid := q.Get("RID")

	if sid == "" && rid != "" {
		sess := h.mgr.NewSession()
		reqs, _ := parseWriteForwardBody(r)

		ctx, cancel := context.WithCancel(context.Background())
		bridge := newWriteBridge(ctx)

		sess.writeBridge = bridge
		sess.cancel = cancel

		// gRPC handler goroutine.
		go func() {
			defer cancel()
			defer h.mgr.Remove(sess.ID)
			_ = h.writeFn(bridge)
		}()

		// Pump goroutine: reads WriteResponses and appends JSON chunks to session log.
		go func() {
			for {
				select {
				case resp, ok := <-bridge.sendCh:
					if !ok {
						return
					}
					b, err := protojson.Marshal(resp)
					if err != nil {
						continue
					}
					chunk := sess.FormatDataChunk(json.RawMessage(b))
					sess.writeLog.Append(chunk)
				case <-bridge.ctx.Done():
					return
				}
			}
		}()

		for _, req := range reqs {
			select {
			case bridge.recvCh <- req:
			default:
			}
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(sess.FormatConnectChunk())
		return
	}

	sess := h.mgr.Get(sid)
	if sess == nil {
		log.Printf("[webchannel] Write POST sid=%s not found", sid)
		http.Error(w, "session not found", http.StatusBadRequest)
		return
	}
	if !sess.SeenRID(rid) {
		reqs, _ := parseWriteForwardBody(r)
		log.Printf("[webchannel] Write POST sid=%s rid=%s parsed=%d reqs", sid, rid, len(reqs))
		for _, req := range reqs {
			log.Printf("[webchannel] Write req writes=%d", len(req.GetWrites()))
			select {
			case sess.writeBridge.recvCh <- req:
			default:
				log.Printf("[webchannel] Write recvCh full, dropping req")
			}
		}
	} else {
		log.Printf("[webchannel] Write POST sid=%s rid=%s duplicate (skipped)", sid, rid)
	}
	w.WriteHeader(http.StatusOK)
}

func (h *WriteHandler) handleBack(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("SID")
	sess := h.mgr.Get(sid)
	if sess == nil {
		log.Printf("[webchannel] Write GET back-channel sid=%s not found", sid)
		http.Error(w, "session not found", http.StatusBadRequest)
		return
	}

	logIdx := 0
	if aidStr := r.URL.Query().Get("AID"); aidStr != "" {
		if n, err := strconv.Atoi(aidStr); err == nil && n >= 2 {
			logIdx = n - 1
		}
	}
	log.Printf("[webchannel] Write GET back-channel sid=%s AID=%s logIdx=%d", sid, r.URL.Query().Get("AID"), logIdx)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	keepAlive := time.NewTicker(25 * time.Second)
	defer keepAlive.Stop()

	for {
		chunks, notify := sess.writeLog.From(logIdx)
		for _, chunk := range chunks {
			_, _ = w.Write(chunk)
			flusher.Flush()
		}
		logIdx += len(chunks)

		select {
		case <-r.Context().Done():
			return
		case <-sess.writeBridge.ctx.Done():
			final, _ := sess.writeLog.From(logIdx)
			for _, c := range final {
				_, _ = w.Write(c)
				flusher.Flush()
			}
			return
		case <-notify:
			// New data in log — loop to drain it.
		case <-keepAlive.C:
			_, _ = w.Write(sess.FormatNoopChunk())
			flusher.Flush()
		}
	}
}

// parseForwardBody decodes form-encoded BrowserChannel forward channel body.
// Body format: count=N&ofs=M&req0___data__=<json>&req1___data__=<json>...
// WebChannel uses sendRawJson:true so each req___data__ value is a proto3-JSON string.
func parseForwardBody(r *http.Request) ([]*firestorev1.ListenRequest, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	count := 0
	for k := range r.PostForm {
		if strings.HasSuffix(k, "___data__") {
			count++
		}
	}
	reqs := make([]*firestorev1.ListenRequest, 0, count)
	for i := 0; i < count; i++ {
		raw := r.FormValue(fmt.Sprintf("req%d___data__", i))
		if raw == "" {
			continue
		}
		req := &firestorev1.ListenRequest{}
		if err := protojson.Unmarshal([]byte(raw), req); err != nil {
			continue
		}
		reqs = append(reqs, req)
	}
	return reqs, nil
}

// parseWriteForwardBody is like parseForwardBody but decodes WriteRequest protos.
func parseWriteForwardBody(r *http.Request) ([]*firestorev1.WriteRequest, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	count := 0
	for k := range r.PostForm {
		if strings.HasSuffix(k, "___data__") {
			count++
		}
	}
	reqs := make([]*firestorev1.WriteRequest, 0, count)
	for i := 0; i < count; i++ {
		raw := r.FormValue(fmt.Sprintf("req%d___data__", i))
		if raw == "" {
			continue
		}
		req := &firestorev1.WriteRequest{}
		if err := protojson.Unmarshal([]byte(raw), req); err != nil {
			continue
		}
		reqs = append(reqs, req)
	}
	return reqs, nil
}
