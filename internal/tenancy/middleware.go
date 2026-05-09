package tenancy

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/petereon/embyr/internal/registry"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ParseTenantKey extracts (projectID, databaseID) from a Firestore resource path.
// Accepts "projects/p/databases/d/documents/..." or "projects/p/databases/d",
// as well as Firestore REST URL paths with a leading "/v1/" prefix such as
// "/v1/projects/p/databases/d/documents/...".
func ParseTenantKey(path string) (projectID, databaseID string, ok bool) {
	// Strip the REST API version prefix used by Firestore REST and gRPC-gateway URLs.
	// Proto message fields (name, parent, database) never carry this prefix, so the
	// strip is a no-op for gRPC paths.
	path = strings.TrimPrefix(path, "/v1/")
	path = strings.TrimPrefix(path, "v1/")

	parts := strings.SplitN(path, "/", 5)
	if len(parts) < 4 {
		return "", "", false
	}
	if parts[0] != "projects" || parts[2] != "databases" {
		return "", "", false
	}
	if parts[1] == "" || parts[3] == "" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// extractPathFromRequest inspects proto message fields "name", "parent",
// or "database" to find a Firestore resource path.
func extractPathFromRequest(req interface{}) string {
	msg, ok := req.(proto.Message)
	if !ok {
		return ""
	}
	r := msg.ProtoReflect()
	for _, name := range []protoreflect.Name{"name", "parent", "database"} {
		fd := r.Descriptor().Fields().ByName(name)
		if fd == nil || fd.Kind() != protoreflect.StringKind {
			continue
		}
		if v := r.Get(fd).String(); v != "" {
			return v
		}
	}
	return ""
}

// UnaryInterceptor returns a gRPC unary interceptor that resolves the tenant
// adapter from the request path and injects it into the context.
func UnaryInterceptor(factory FactoryLike, log *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx, err := resolveTenant(ctx, factory, log, extractPathFromRequest(req))
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamInterceptor returns a gRPC stream interceptor that peeks at the first
// message to extract the tenant key.
func StreamInterceptor(factory FactoryLike, log *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrapped := &peekStream{ServerStream: ss, factory: factory, log: log}
		return handler(srv, wrapped)
	}
}

type peekStream struct {
	grpc.ServerStream
	factory  FactoryLike
	log      *zap.Logger
	resolved bool
}

func (s *peekStream) RecvMsg(m interface{}) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	if !s.resolved {
		s.resolved = true
		ctx, err := resolveTenant(s.Context(), s.factory, s.log, extractPathFromRequest(m))
		if err != nil {
			return err
		}
		s.ServerStream = &contextStream{ServerStream: s.ServerStream, ctx: ctx}
	}
	return nil
}

func (s *peekStream) Context() context.Context { return s.ServerStream.Context() }

type contextStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextStream) Context() context.Context { return s.ctx }

// HTTPMiddleware wraps an http.Handler, extracting the tenant key from the
// URL path and injecting the adapter into the request context.
func HTTPMiddleware(factory FactoryLike, log *zap.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, err := resolveTenant(r.Context(), factory, log, r.URL.Path)
		if err != nil {
			st, _ := status.FromError(err)
			http.Error(w, st.Message(), grpcCodeToHTTPStatus(st.Code()))
			return
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// resolveTenant looks up the tenant from the path and injects adapter + AuthInfo.
// Both "tenant not found" and "tenant suspended" return codes.Unauthenticated
// with a generic message to prevent tenant enumeration.
func resolveTenant(ctx context.Context, factory FactoryLike, log *zap.Logger, path string) (context.Context, error) {
	projectID, databaseID, ok := ParseTenantKey(path)
	if !ok {
		return ctx, nil // Task 3 will make this fail-closed
	}

	adapter, authCfg, rawAuthCfg, err := factory.Get(ctx, projectID, databaseID)
	if err != nil {
		if errors.Is(err, ErrTenantSuspended) || errors.Is(err, registry.ErrTenantNotFound) {
			return ctx, status.Error(codes.Unauthenticated, "unauthenticated")
		}
		log.Warn("tenancy: factory error", zap.Error(err),
			zap.String("project", projectID), zap.String("database", databaseID))
		return ctx, status.Error(codes.Internal, "internal error")
	}

	ctx = WithAdapter(ctx, adapter)
	ctx = WithAuthInfo(ctx, AuthInfo{
		Mode:   authCfg.Mode(),
		Config: rawAuthCfg, // now correctly populated from registry tenant.AuthConfig
	})
	return ctx, nil
}

func grpcCodeToHTTPStatus(c codes.Code) int {
	switch c {
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	default:
		return http.StatusInternalServerError
	}
}
