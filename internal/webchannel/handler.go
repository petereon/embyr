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

	firestorev1 "github.com/petereon/embyr/gen/go/google/firestore/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// TenantResolveFn resolves and authenticates a tenant from a Firestore resource path.
// Returning a non-nil error aborts session establishment with the appropriate HTTP status.
// Set to nil in single-tenant mode (no-op).
type TenantResolveFn func(ctx context.Context, path string) (context.Context, error)

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
func (b *listenBridge) SetTrailer(metadata.MD)       {}
func (b *listenBridge) Context() context.Context     { return b.ctx }
func (b *listenBridge) SendMsg(m any) error          { return nil }
func (b *listenBridge) RecvMsg(m any) error          { return nil }

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
	mgr       *Manager
	listenFn  func(firestorev1.Firestore_ListenServer) error
	resolveFn TenantResolveFn // nil in single-tenant mode
}

// NewHandler creates a BrowserChannel Handler. Pass resolveFn=nil in single-tenant mode.
func NewHandler(mgr *Manager, listenFn func(firestorev1.Firestore_ListenServer) error, resolveFn TenantResolveFn) *Handler {
	return &Handler{mgr: mgr, listenFn: listenFn, resolveFn: resolveFn}
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
		// Parse body first so we can extract the database path for tenant resolution.
		reqs, _ := parseForwardBody(r)

		ctx := r.Context()
		if h.resolveFn != nil {
			var database string
			if len(reqs) > 0 {
				database = reqs[0].GetDatabase()
			}
			// Empty database is safe: resolveFn's fail-closed path rejects it as Unauthenticated.
			// Inject HTTP Authorization header as gRPC incoming metadata so
			// auth.ValidateForTenant → bearerToken(ctx) works correctly.
			if authHeader := r.Header.Get("Authorization"); authHeader != "" {
				md := metadata.New(map[string]string{"authorization": authHeader})
				ctx = metadata.NewIncomingContext(ctx, md)
			}
			var err error
			ctx, err = h.resolveFn(ctx, database)
			if err != nil {
				c := status.Code(err)
				http.Error(w, status.Convert(err).Message(), wcGRPCToHTTP(c))
				return
			}
		}

		sess := h.mgr.NewSession()
		// Detach from the HTTP request's cancellation: the bridge must outlive
		// this POST handler, but must carry the authenticated values (adapter,
		// tenant key, etc.) from ctx.
		sessCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		bridge := newListenBridge(sessCtx)

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
						log.Printf("[webchannel] listen pump: marshal error (terminating session): %v", err)
						cancel()
						return
					}
					chunk := sess.FormatDataChunk(json.RawMessage(b))
					sess.listenLog.Append(chunk)
				case <-bridge.ctx.Done():
					return
				}
			}
		}()

		for _, req := range reqs {
			select {
			case bridge.recvCh <- req:
			case <-r.Context().Done():
				return
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
	if sess == nil || sess.GetListenBridge() == nil {
		http.Error(w, "session not found", http.StatusBadRequest)
		return
	}
	bridge := sess.GetListenBridge()
	if !sess.SeenRID(rid) {
		reqs, _ := parseForwardBody(r)
		for _, req := range reqs {
			select {
			case bridge.recvCh <- req:
			case <-r.Context().Done():
				return
			}
		}
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(formatForwardPostStatus(sess))
}

// formatForwardPostStatus returns the body for a WebChannel forward POST
// response. The SDK reads the body via the same chunk extractor it uses for
// the back-channel — `<length>\n<json>` — and the JSON must be a 3-element
// array of [lastArrayId, outstandingBytes, unused] (see Rb in
// @firebase/webchannel-wrapper). A bare JSON body, or an empty body, leaves
// the SDK's chunk extractor in "incomplete" state, which marks the forward
// request as failed and blocks every subsequent send on this WebChannel
// session — manifesting as filter switches that hang or session reconnects
// every ~10 seconds.
func formatForwardPostStatus(sess *Session) []byte {
	json := fmt.Sprintf("[%d,0,0]", sess.DataSeq()-1)
	return []byte(fmt.Sprintf("%d\n%s", len(json), json))
}

// handleBack handles GET /Listen/channel — the long-polling back channel.
// Multiple concurrent connections (CI=0, CI=1, …) are each served independently
// from the session's message log, so no message is split between connections.
func (h *Handler) handleBack(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("SID")
	sess := h.mgr.Get(sid)
	if sess == nil || sess.GetListenBridge() == nil {
		// Either no such session or it belongs to the Write channel.
		http.Error(w, "session not found", http.StatusBadRequest)
		return
	}
	bridge := sess.GetListenBridge()

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
		case <-bridge.ctx.Done():
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
			// Noops claim seqs from the same global counter as data chunks so
			// no two chunks in a session ever share a seq.
			_, _ = w.Write(sess.FormatNoopChunk(sess.NextNoopSeq()))
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
func (b *writeBridge) SetTrailer(metadata.MD)       {}
func (b *writeBridge) Context() context.Context     { return b.ctx }
func (b *writeBridge) SendMsg(m any) error          { return nil }
func (b *writeBridge) RecvMsg(m any) error          { return nil }

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
	mgr       *Manager
	writeFn   func(firestorev1.Firestore_WriteServer) error
	resolveFn TenantResolveFn
}

// NewWriteHandler creates a WriteHandler. Pass resolveFn=nil in single-tenant mode.
func NewWriteHandler(mgr *Manager, writeFn func(firestorev1.Firestore_WriteServer) error, resolveFn TenantResolveFn) *WriteHandler {
	return &WriteHandler{mgr: mgr, writeFn: writeFn, resolveFn: resolveFn}
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
		// Parse body first so we can extract the database path for tenant resolution.
		reqs, _ := parseWriteForwardBody(r)

		ctx := r.Context()
		if h.resolveFn != nil {
			var database string
			if len(reqs) > 0 {
				database = reqs[0].GetDatabase()
			}
			// Empty database is safe: resolveFn's fail-closed path rejects it as Unauthenticated.
			// Inject HTTP Authorization header as gRPC incoming metadata so
			// auth.ValidateForTenant → bearerToken(ctx) works correctly.
			if authHeader := r.Header.Get("Authorization"); authHeader != "" {
				md := metadata.New(map[string]string{"authorization": authHeader})
				ctx = metadata.NewIncomingContext(ctx, md)
			}
			var err error
			ctx, err = h.resolveFn(ctx, database)
			if err != nil {
				c := status.Code(err)
				http.Error(w, status.Convert(err).Message(), wcGRPCToHTTP(c))
				return
			}
		}

		sess := h.mgr.NewSession()
		// Detach from the HTTP request's cancellation: the bridge must outlive
		// this POST handler, but must carry the authenticated values (adapter,
		// tenant key, etc.) from ctx.
		sessCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		bridge := newWriteBridge(sessCtx)

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
						log.Printf("[webchannel] write pump: marshal error (terminating session): %v", err)
						cancel()
						return
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
			case <-r.Context().Done():
				return
			}
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(sess.FormatConnectChunk())
		return
	}

	sess := h.mgr.Get(sid)
	if sess == nil || sess.GetWriteBridge() == nil {
		http.Error(w, "session not found", http.StatusBadRequest)
		return
	}
	wb := sess.GetWriteBridge()
	if !sess.SeenRID(rid) {
		reqs, _ := parseWriteForwardBody(r)
		for _, req := range reqs {
			select {
			case wb.recvCh <- req:
			case <-r.Context().Done():
				return
			}
		}
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(formatForwardPostStatus(sess))
}

func (h *WriteHandler) handleBack(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("SID")
	sess := h.mgr.Get(sid)
	if sess == nil || sess.GetWriteBridge() == nil {
		http.Error(w, "session not found", http.StatusBadRequest)
		return
	}
	wb := sess.GetWriteBridge()

	logIdx := 0
	if aidStr := r.URL.Query().Get("AID"); aidStr != "" {
		if n, err := strconv.Atoi(aidStr); err == nil && n >= 2 {
			logIdx = n - 1
		}
	}
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
		case <-wb.ctx.Done():
			final, _ := sess.writeLog.From(logIdx)
			for _, c := range final {
				_, _ = w.Write(c)
				flusher.Flush()
			}
			return
		case <-notify:
			// New data in log — loop to drain it.
		case <-keepAlive.C:
			_, _ = w.Write(sess.FormatNoopChunk(sess.NextNoopSeq()))
			flusher.Flush()
		}
	}
}

// wcGRPCToHTTP maps gRPC status codes to HTTP status codes for WebChannel responses.
// A separate helper (not imported from tenancy) is needed to avoid a dependency cycle.
func wcGRPCToHTTP(c codes.Code) int {
	switch c {
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	default:
		return http.StatusInternalServerError
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
