package auth

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"

	"github.com/petereon/firstyr/internal/config"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

var tokenInfoURLAtomic atomic.Value

func init() {
	tokenInfoURLAtomic.Store("https://www.googleapis.com/oauth2/v1/tokeninfo")
}

// SetTokenInfoURL overrides the Google tokeninfo endpoint. For testing only.
func SetTokenInfoURL(url string) { tokenInfoURLAtomic.Store(url) }

func getTokenInfoURL() string { return tokenInfoURLAtomic.Load().(string) }

// Config holds the auth interceptors and optional TLS credentials for the gRPC server.
type Config struct {
	// Unary is the unary server interceptor for the configured auth mode.
	Unary grpc.UnaryServerInterceptor
	// Stream is the stream server interceptor for the configured auth mode.
	Stream grpc.StreamServerInterceptor
	// Creds is the transport credentials to apply to the gRPC server.
	// Nil for all modes except "mtls".
	Creds credentials.TransportCredentials
}

// New builds auth interceptors from the given configuration.
// Returns an error if the configuration is invalid (e.g. key mode with no key set).
func New(cfg *config.Config, _ *zap.Logger) (*Config, error) {
	switch cfg.Auth.Mode {
	case "none", "":
		return &Config{Unary: noneUnary, Stream: noneStream}, nil

	case "key":
		if cfg.Auth.Key == "" {
			return nil, fmt.Errorf("auth: key mode requires auth.key to be set")
		}
		return &Config{
			Unary:  keyUnary(cfg.Auth.Key),
			Stream: keyStream(cfg.Auth.Key),
		}, nil

	case "google":
		if cfg.Auth.GoogleProjectID == "" {
			return nil, fmt.Errorf("auth: google mode requires auth.google_project_id to be set")
		}
		return &Config{
			Unary:  googleUnary(cfg.Auth.GoogleProjectID),
			Stream: googleStream(cfg.Auth.GoogleProjectID),
		}, nil

	case "mtls":
		creds, err := buildMTLSCreds(cfg)
		if err != nil {
			return nil, err
		}
		return &Config{
			Unary:  mtlsUnary,
			Stream: mtlsStream,
			Creds:  creds,
		}, nil

	default:
		return nil, fmt.Errorf("auth: unknown mode %q", cfg.Auth.Mode)
	}
}

// ─── none ────────────────────────────────────────────────────────────────────

func noneUnary(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return handler(ctx, req)
}

func noneStream(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return handler(srv, ss)
}

// ─── key ─────────────────────────────────────────────────────────────────────

func keyUnary(key string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := validateKey(ctx, key); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func keyStream(key string) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := validateKey(ss.Context(), key); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

func validateKey(ctx context.Context, expectedKey string) error {
	token, err := bearerToken(ctx)
	if err != nil {
		return err
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(expectedKey)) != 1 {
		return status.Error(codes.Unauthenticated, "invalid API key")
	}
	return nil
}

// ─── google ───────────────────────────────────────────────────────────────────

func googleUnary(projectID string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := validateGoogleToken(ctx, projectID); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func googleStream(projectID string) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := validateGoogleToken(ss.Context(), projectID); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

func validateGoogleToken(ctx context.Context, projectID string) error {
	token, err := bearerToken(ctx)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		getTokenInfoURL()+"?id_token="+token, nil)
	if err != nil {
		return status.Errorf(codes.Internal, "google token request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "google token validation: %v", err)
	}
	defer func() {
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		resp.Body.Close()             //nolint:errcheck
	}()
	if resp.StatusCode != http.StatusOK {
		return status.Error(codes.Unauthenticated, "invalid google token")
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return status.Errorf(codes.Internal, "google token read body: %v", err)
	}
	var info struct {
		Audience string `json:"audience"`
		AZP      string `json:"azp"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return status.Errorf(codes.Internal, "google token parse: %v", err)
	}
	if info.Audience != projectID && info.AZP != projectID {
		return status.Error(codes.Unauthenticated, "token audience does not match project ID")
	}
	return nil
}

// ─── mtls ─────────────────────────────────────────────────────────────────────

func buildMTLSCreds(cfg *config.Config) (credentials.TransportCredentials, error) {
	if cfg.Auth.MTLSCACert == "" {
		return nil, fmt.Errorf("auth: mtls mode requires auth.mtls_ca to be set")
	}
	if cfg.Server.TLS.Cert == "" || cfg.Server.TLS.Key == "" {
		return nil, fmt.Errorf("auth: mtls mode requires server.tls.cert and server.tls.key to be set")
	}
	caCert, err := os.ReadFile(cfg.Auth.MTLSCACert)
	if err != nil {
		return nil, fmt.Errorf("auth: read mTLS CA cert: %w", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("auth: invalid mTLS CA cert PEM")
	}
	serverCert, err := tls.LoadX509KeyPair(cfg.Server.TLS.Cert, cfg.Server.TLS.Key)
	if err != nil {
		return nil, fmt.Errorf("auth: load server cert/key: %w", err)
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
	}), nil
}

func mtlsUnary(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if err := validateMTLS(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func mtlsStream(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := validateMTLS(ss.Context()); err != nil {
		return err
	}
	return handler(srv, ss)
}

func validateMTLS(ctx context.Context) error {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "no peer info in context")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return status.Error(codes.Unauthenticated, "connection is not using TLS")
	}
	if len(tlsInfo.State.VerifiedChains) == 0 {
		return status.Error(codes.Unauthenticated, "no verified client certificate")
	}
	return nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// bearerToken extracts the Bearer token from incoming gRPC metadata.
func bearerToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing authorization header")
	}
	parts := strings.SplitN(vals[0], " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return "", status.Error(codes.Unauthenticated, "authorization header must be 'Bearer <token>'")
	}
	return parts[1], nil
}
