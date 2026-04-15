package server

import (
	"context"
	"fmt"
	"net"
	"net/http"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"github.com/petereon/firstyr/internal/config"
	"github.com/petereon/firstyr/internal/health"
	"github.com/petereon/firstyr/internal/store"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// firestoreServer is the gRPC service implementation.
// All RPCs return UNIMPLEMENTED until Plans 2-4 fill them in.
type firestoreServer struct {
	firestorev1.UnimplementedFirestoreServer
}

// Server wraps the gRPC server and the grpc-gateway REST mux.
type Server struct {
	cfg        *config.Config
	db         store.StorageAdapter
	log        *zap.Logger
	grpcServer *grpc.Server
	restMux    *http.ServeMux
}

// New creates a Server wired to db. It does not start listening.
func New(cfg *config.Config, db store.StorageAdapter, log *zap.Logger) (*Server, error) {
	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(loggingUnaryInterceptor(log)),
		grpc.StreamInterceptor(loggingStreamInterceptor(log)),
	)
	firestorev1.RegisterFirestoreServer(grpcSrv, &firestoreServer{})
	reflection.Register(grpcSrv)

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
	mux.Handle("/", gwMux)

	return &Server{
		cfg:        cfg,
		db:         db,
		log:        log,
		grpcServer: grpcSrv,
		restMux:    mux,
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

	restSrv := &http.Server{Handler: s.restMux}

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error { return s.grpcServer.Serve(grpcLis) })
	eg.Go(func() error {
		if err := restSrv.Serve(restLis); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})
	eg.Go(func() error {
		<-egCtx.Done()
		s.grpcServer.GracefulStop()
		return restSrv.Shutdown(context.Background())
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
