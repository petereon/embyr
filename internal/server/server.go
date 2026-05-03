package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	firestorev1 "github.com/petereon/embyr/gen/go/google/firestore/v1"
	"github.com/petereon/embyr/internal/auth"
	"github.com/petereon/embyr/internal/config"
	"github.com/petereon/embyr/internal/health"
	"github.com/petereon/embyr/internal/listen"
	"github.com/petereon/embyr/internal/store"
	"github.com/petereon/embyr/internal/tenancy"
	"github.com/petereon/embyr/internal/webchannel"
	"github.com/rs/cors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// Server wraps the gRPC server, the grpc-gateway REST mux, and the gRPC-Web wrapper.
type Server struct {
	cfg        *config.Config
	db         store.StorageAdapter
	log        *zap.Logger
	grpcServer *grpc.Server
	restMux    *http.ServeMux
	grpcWebSrv *grpcweb.WrappedGrpcServer
	fs         *firestoreServer
	wcMgr      *webchannel.Manager
}

// New creates a Server wired to db. It does not start listening.
func New(cfg *config.Config, db store.StorageAdapter, log *zap.Logger) (*Server, error) {
	authCfg, err := auth.New(cfg, log)
	if err != nil {
		return nil, fmt.Errorf("server: auth: %w", err)
	}

	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(loggingUnaryInterceptor(log), authCfg.Unary),
		grpc.ChainStreamInterceptor(loggingStreamInterceptor(log), authCfg.Stream),
	}
	if authCfg.Creds != nil {
		opts = append(opts, grpc.Creds(authCfg.Creds))
	}

	grpcSrv := grpc.NewServer(opts...)
	reg := listen.NewRegistry()
	fs := &firestoreServer{db: db, log: log, registry: reg}
	firestorev1.RegisterFirestoreServer(grpcSrv, fs)
	reflection.Register(grpcSrv)

	// gRPC-Web wraps the gRPC server so browsers can call it over HTTP/1.1.
	// It is served on the same REST port alongside grpc-gateway.
	grpcWebSrv := grpcweb.WrapServer(grpcSrv,
		grpcweb.WithOriginFunc(func(origin string) bool { return true }),
	)

	gwMux := runtime.NewServeMux()
	grpcAddr := fmt.Sprintf("127.0.0.1:%d", cfg.Server.GRPCPort)
	if err := firestorev1.RegisterFirestoreHandlerFromEndpoint(
		context.Background(), gwMux, grpcAddr,
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	); err != nil {
		return nil, fmt.Errorf("server: register gateway: %w", err)
	}

	h := health.New(db)
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.Healthz)
	mux.HandleFunc("/readyz", h.Readyz)

	wcMgr := webchannel.NewManager()
	wcHandler := webchannel.NewHandler(wcMgr, fs.Listen)
	mux.Handle("/google.firestore.v1.Firestore/Listen/channel", wcHandler)
	wcWriteHandler := webchannel.NewWriteHandler(wcMgr, fs.Write)
	mux.Handle("/google.firestore.v1.Firestore/Write/channel", wcWriteHandler)

	// Intercept :runQuery before grpc-gateway. The Firebase lite SDK calls
	// JSON.parse() on the full body and expects a JSON array, but grpc-gateway
	// emits concatenated NDJSON objects {"result":{…}}. serveRunQuery collects
	// all stream messages and returns a proper JSON array instead.
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":runQuery") {
			serveRunQuery(w, r, fs)
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":batchGet") {
			serveBatchGetDocuments(w, r, fs)
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":runAggregationQuery") {
			serveRunAggregationQuery(w, r, fs)
			return
		}
		gwMux.ServeHTTP(w, r)
	}))

	return &Server{
		cfg:        cfg,
		db:         db,
		log:        log,
		grpcServer: grpcSrv,
		restMux:    mux,
		grpcWebSrv: grpcWebSrv,
		fs:         fs,
		wcMgr:      wcMgr,
	}, nil
}

// NewMultiTenant creates a Server in multi-tenant mode. Each Firestore request
// is routed to the customer's own Postgres DB via AdapterFactory. db is nil;
// per-tenant adapters are injected into context by the tenancy interceptors.
func NewMultiTenant(cfg *config.Config, factory *tenancy.AdapterFactory, log *zap.Logger) (*Server, error) {
	authCfg := &auth.Config{
		Unary:  tenancy.UnaryInterceptor(factory, log),
		Stream: tenancy.StreamInterceptor(factory, log),
	}

	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(loggingUnaryInterceptor(log), authCfg.Unary),
		grpc.ChainStreamInterceptor(loggingStreamInterceptor(log), authCfg.Stream),
	}

	grpcSrv := grpc.NewServer(opts...)
	reg := listen.NewRegistry()
	fs := &firestoreServer{db: nil, log: log, registry: reg}
	firestorev1.RegisterFirestoreServer(grpcSrv, fs)
	reflection.Register(grpcSrv)

	grpcWebSrv := grpcweb.WrapServer(grpcSrv,
		grpcweb.WithOriginFunc(func(origin string) bool { return true }),
	)

	gwMux := runtime.NewServeMux()
	grpcAddr := fmt.Sprintf("127.0.0.1:%d", cfg.Server.GRPCPort)
	if err := firestorev1.RegisterFirestoreHandlerFromEndpoint(
		context.Background(), gwMux, grpcAddr,
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	); err != nil {
		return nil, fmt.Errorf("server: register gateway: %w", err)
	}

	h := health.New(nil)
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.Healthz)
	mux.HandleFunc("/readyz", h.Readyz)

	wcMgr := webchannel.NewManager()
	wcHandler := webchannel.NewHandler(wcMgr, fs.Listen)
	mux.Handle("/google.firestore.v1.Firestore/Listen/channel", wcHandler)
	wcWriteHandler := webchannel.NewWriteHandler(wcMgr, fs.Write)
	mux.Handle("/google.firestore.v1.Firestore/Write/channel", wcWriteHandler)

	innerMux := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":runQuery") {
			serveRunQuery(w, r, fs)
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":batchGet") {
			serveBatchGetDocuments(w, r, fs)
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":runAggregationQuery") {
			serveRunAggregationQuery(w, r, fs)
			return
		}
		gwMux.ServeHTTP(w, r)
	})
	mux.Handle("/", tenancy.HTTPMiddleware(factory, log, innerMux))

	return &Server{
		cfg:        cfg,
		db:         nil,
		log:        log,
		grpcServer: grpcSrv,
		restMux:    mux,
		grpcWebSrv: grpcWebSrv,
		fs:         fs,
		wcMgr:      wcMgr,
	}, nil
}

// Run starts both the gRPC and REST listeners. Blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cfg.Server.GRPCPort))
	if err != nil {
		return fmt.Errorf("server: gRPC listen: %w", err)
	}
	// If REST listener allocation fails, close the gRPC listener so the port
	// is released immediately rather than relying on the GC finalizer.
	restLis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cfg.Server.RESTPort))
	if err != nil {
		_ = grpcLis.Close()
		return fmt.Errorf("server: REST listen: %w", err)
	}

	// timedRestMux wraps the REST gateway with a 30s write timeout so slow-read
	// clients cannot hold REST connections open indefinitely. gRPC-Web and
	// BrowserChannel requests bypass this and use the server's WriteTimeout:0.
	timedRestMux := http.TimeoutHandler(s.restMux, 30*time.Second, "gateway timeout")
	// combinedHandler routes streaming requests (gRPC-Web, BrowserChannel) to
	// their handlers without a write deadline, and all other REST requests
	// through the timed REST mux to enforce a 30s response deadline.
	combinedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.grpcWebSrv.IsGrpcWebRequest(r) || s.grpcWebSrv.IsAcceptableGrpcCorsRequest(r) {
			s.grpcWebSrv.ServeHTTP(w, r)
			return
		}
		// BrowserChannel paths need chunked HTTP streaming — bypass the timeout wrapper.
		if strings.HasSuffix(r.URL.Path, "/channel") {
			s.restMux.ServeHTTP(w, r)
			return
		}
		timedRestMux.ServeHTTP(w, r)
	})
	// corsHandler enforces CORS. AllowedOrigins=[] (default) allows any origin
	// so the Firebase JS SDK works in local development; set allowed_origins in
	// config for network-exposed deployments to restrict access.
	corsOpts := cors.Options{
		AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"*"},
		AllowCredentials: true,
	}
	if len(s.cfg.Server.AllowedOrigins) > 0 {
		corsOpts.AllowedOrigins = s.cfg.Server.AllowedOrigins
	} else {
		corsOpts.AllowOriginFunc = func(string) bool { return true }
	}
	corsHandler := cors.New(corsOpts).Handler(combinedHandler)

	restSrv := &http.Server{
		Handler:           corsHandler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      0, // disabled: gRPC-Web streams can be long-lived
	}

	eg, egCtx := errgroup.WithContext(ctx)
	// Feed registry from adapter subscription; tied to egCtx so it stops on shutdown.
	eg.Go(func() error {
		if s.db == nil {
			return nil
		}
		ch, cancel := s.db.Subscribe(egCtx)
		defer cancel()
		for c := range ch {
			s.fs.registry.Dispatch(c)
		}
		return nil
	})
	eg.Go(func() error { return s.grpcServer.Serve(grpcLis) })
	eg.Go(func() error {
		if err := restSrv.Serve(restLis); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})
	// Honor the configured sweep interval (default 30s when not set).
	sweepInterval := s.cfg.Transactions.SweepInterval
	if sweepInterval <= 0 {
		sweepInterval = 30 * time.Second
	}
	eg.Go(func() error {
		if s.db == nil {
			return nil
		}
		ticker := time.NewTicker(sweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n, err := s.db.SweepExpiredTransactions(egCtx)
				if err != nil {
					s.log.Warn("transaction sweep failed", zap.Error(err))
				} else if n > 0 {
					s.log.Info("swept expired transactions", zap.Int("deleted", n))
				}
			case <-egCtx.Done():
				return nil
			}
		}
	})
	eg.Go(func() error {
		<-egCtx.Done()
		// Cancel any active BrowserChannel sessions so their gRPC handler
		// goroutines (parented on context.Background) exit cleanly instead of
		// outliving the HTTP server.
		s.wcMgr.Shutdown()
		s.grpcServer.GracefulStop()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return restSrv.Shutdown(shutCtx)
	})
	return eg.Wait()
}

func loggingUnaryInterceptor(log *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		log.Info("rpc", zap.String("method", info.FullMethod), zap.String("code", status.Code(err).String()))
		return resp, err
	}
}

func loggingStreamInterceptor(log *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		log.Info("rpc_stream", zap.String("method", info.FullMethod), zap.String("code", status.Code(err).String()))
		return err
	}
}

// serveRunQuery handles POST …/:runQuery requests by streaming a JSON array
// response that matches real Firestore REST behaviour: bytes are flushed to
// the client as each document is ready rather than buffered until the query
// completes. The array is opened with `[`, items are comma-separated, and
// closed with `]` once RunQuery returns.
func serveRunQuery(w http.ResponseWriter, r *http.Request, fs *firestoreServer) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Extract parent from URL: /v1/<parent>:runQuery → <parent>
	parent := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/"), ":runQuery")

	req := &firestorev1.RunQueryRequest{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(body, req); err != nil {
		http.Error(w, "decode request: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.Parent = parent

	flusher, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "application/json")
	streamer := &runQueryStreamer{
		ctx:     r.Context(),
		w:       w,
		flusher: flusher,
		marshal: protojson.MarshalOptions{EmitUnpopulated: false},
	}

	if rpcErr := fs.RunQuery(req, streamer); rpcErr != nil {
		if !streamer.started {
			// Nothing written yet — can still send a proper HTTP error.
			code := status.Code(rpcErr)
			http.Error(w, rpcErr.Error(), grpcCodeToHTTP(code))
			return
		}
		// Body already started; append an error element so the client can detect failure.
		grpcCode := status.Code(rpcErr)
		fmt.Fprintf(w, `,{"error":{"code":%d,"message":%q,"status":"%s"}}]`,
			grpcCodeToHTTP(grpcCode), rpcErr.Error(), grpcCode.String())
		return
	}

	if !streamer.started {
		// RunQuery succeeded but sent nothing (empty collection).
		w.Write([]byte("["))
	}
	w.Write([]byte("]"))
}

// serveRunAggregationQuery handles POST …/:runAggregationQuery requests.
// grpc-gateway emits NDJSON; the Lite SDK calls JSON.parse() and expects a
// single JSON array, so we intercept, call RunAggregationQuery directly, and
// return [<response>].
func serveRunAggregationQuery(w http.ResponseWriter, r *http.Request, fs *firestoreServer) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	parent := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/"), ":runAggregationQuery")

	req := &firestorev1.RunAggregationQueryRequest{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(body, req); err != nil {
		http.Error(w, "decode request: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.Parent = parent

	marshal := protojson.MarshalOptions{EmitUnpopulated: false}
	var result []byte

	streamer := &aggQueryStreamer{
		ctx: r.Context(),
		onSend: func(resp *firestorev1.RunAggregationQueryResponse) {
			result, _ = marshal.Marshal(resp)
		},
	}

	if rpcErr := fs.RunAggregationQuery(req, streamer); rpcErr != nil {
		code := status.Code(rpcErr)
		http.Error(w, rpcErr.Error(), grpcCodeToHTTP(code))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if result == nil {
		w.Write([]byte("[]"))
		return
	}
	w.Write([]byte("["))
	w.Write(result)
	w.Write([]byte("]"))
}

type aggQueryStreamer struct {
	ctx    context.Context
	onSend func(*firestorev1.RunAggregationQueryResponse)
}

func (s *aggQueryStreamer) Send(r *firestorev1.RunAggregationQueryResponse) error {
	s.onSend(r)
	return nil
}
func (s *aggQueryStreamer) SetHeader(md metadata.MD) error  { return nil }
func (s *aggQueryStreamer) SendHeader(md metadata.MD) error { return nil }
func (s *aggQueryStreamer) SetTrailer(metadata.MD)          {}
func (s *aggQueryStreamer) Context() context.Context        { return s.ctx }
func (s *aggQueryStreamer) SendMsg(m any) error             { return nil }
func (s *aggQueryStreamer) RecvMsg(m any) error             { return nil }

func grpcCodeToHTTP(c codes.Code) int {
	switch c {
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists:
		return http.StatusConflict
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.Unimplemented:
		return http.StatusNotImplemented
	default:
		return http.StatusInternalServerError
	}
}
