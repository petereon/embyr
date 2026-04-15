package webchannel

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
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

		go func() {
			defer cancel()
			defer h.mgr.Remove(sess.ID)
			_ = h.listenFn(bridge)
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

	// Forward channel message for existing session.
	sess := h.mgr.Get(sid)
	if sess == nil {
		http.Error(w, "session not found", http.StatusBadRequest)
		return
	}
	reqs, _ := parseForwardBody(r)
	for _, req := range reqs {
		select {
		case sess.bridge.recvCh <- req:
		default:
		}
	}
	w.WriteHeader(http.StatusOK)
}

// handleBack handles GET /Listen/channel — the long-polling back channel.
func (h *Handler) handleBack(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("SID")
	sess := h.mgr.Get(sid)
	if sess == nil {
		http.Error(w, "session not found", http.StatusBadRequest)
		return
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
		select {
		case <-r.Context().Done():
			return
		case <-sess.bridge.ctx.Done():
			return
		case resp, ok := <-sess.bridge.sendCh:
			if !ok {
				return
			}
			b, err := proto.Marshal(resp)
			if err != nil {
				continue
			}
			frame := EncodeGRPCWebFrame(b)
			chunk := sess.FormatDataChunk(frame)
			_, _ = w.Write(chunk)
			flusher.Flush()
		case <-keepAlive.C:
			_, _ = w.Write(sess.FormatNoopChunk())
			flusher.Flush()
		}
	}
}

// parseForwardBody decodes form-encoded BrowserChannel forward channel body.
// Body format: count=N&ofs=M&req0___data__=<base64>&req1___data__=<base64>...
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
		encoded := r.FormValue(fmt.Sprintf("req%d___data__", i))
		if encoded == "" {
			continue
		}
		frameBytes, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			continue
		}
		protoBytes, err := DecodeGRPCWebFrame(frameBytes)
		if err != nil {
			continue
		}
		req := &firestorev1.ListenRequest{}
		if err := proto.Unmarshal(protoBytes, req); err != nil {
			continue
		}
		reqs = append(reqs, req)
	}
	return reqs, nil
}
