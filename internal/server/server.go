package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"github.com/petereon/firstyr/internal/auth"
	"github.com/petereon/firstyr/internal/config"
	"github.com/petereon/firstyr/internal/health"
	"github.com/petereon/firstyr/internal/listen"
	"github.com/petereon/firstyr/internal/store"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
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
	// Start feeding the registry from the adapter's Subscribe channel.
	go func() {
		ch, cancel := db.Subscribe()
		defer cancel()
		for c := range ch {
			reg.Dispatch(c)
		}
	}()
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
	// Intercept :runQuery before grpc-gateway. The Firebase lite SDK calls
	// JSON.parse() on the full body and expects a JSON array, but grpc-gateway
	// emits concatenated NDJSON objects {"result":{…}}. serveRunQuery collects
	// all stream messages and returns a proper JSON array instead.
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":runQuery") {
			serveRunQuery(w, r, fs)
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
	}, nil
}

// Run starts both the gRPC and REST listeners. Blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cfg.Server.GRPCPort))
	if err != nil {
		return fmt.Errorf("server: gRPC listen: %w", err)
	}
	restLis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cfg.Server.RESTPort))
	if err != nil {
		return fmt.Errorf("server: REST listen: %w", err)
	}

	// combinedHandler routes gRPC-Web requests (application/grpc-web+proto content-type)
	// to the gRPC-Web wrapper, and everything else to the grpc-gateway REST mux.
	// WriteTimeout is 0 so long-running gRPC-Web streams are not killed prematurely;
	// individual RPCs rely on gRPC deadlines for their own timeouts.
	combinedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.grpcWebSrv.IsGrpcWebRequest(r) || s.grpcWebSrv.IsAcceptableGrpcCorsRequest(r) {
			s.grpcWebSrv.ServeHTTP(w, r)
			return
		}
		s.restMux.ServeHTTP(w, r)
	})
	restSrv := &http.Server{
		Handler:           combinedHandler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      0, // disabled: gRPC-Web streams can be long-lived
	}

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error { return s.grpcServer.Serve(grpcLis) })
	eg.Go(func() error {
		if err := restSrv.Serve(restLis); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})
	sweepInterval := 30 * time.Second
	eg.Go(func() error {
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
		// Body already started; close the array and let the client deal with it.
		w.Write([]byte("]"))
		return
	}

	if !streamer.started {
		// RunQuery succeeded but sent nothing (empty collection).
		w.Write([]byte("["))
	}
	w.Write([]byte("]"))
}

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
