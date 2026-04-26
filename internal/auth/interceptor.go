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
	"net/url"
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

// validateGoogleToken verifies a Bearer token against Google's tokeninfo
// endpoint. Bearer tokens may be either:
//   - a Firebase / OIDC ID token (JWT) — sent as `?id_token=…`, validated by
//     audience matching the configured project ID.
//   - an OAuth 2 access token (opaque) — sent as `?access_token=…`, accepted
//     when tokeninfo returns 200 (the response does not carry the project ID,
//     so further authorization is delegated to the application layer).
//
// Try id_token first; if Google rejects (HTTP 4xx), retry as access_token.
func validateGoogleToken(ctx context.Context, projectID string) error {
	token, err := bearerToken(ctx)
	if err != nil {
		return err
	}

	// Attempt 1: ID token validation.
	if ok, info, idErr := googleTokenInfo(ctx, "id_token", token); idErr != nil {
		return idErr
	} else if ok {
		// ID token responses must match the configured project ID.
		if info.Audience != projectID && info.AZP != projectID && info.Aud != projectID {
			return status.Error(codes.Unauthenticated, "token audience does not match project ID")
		}
		return nil
	}

	// Attempt 2: OAuth 2 access token. Google's tokeninfo response for access
	// tokens does not include the project ID, so any 200 from Google means the
	// token is currently valid; finer-grained authorization is the
	// application's responsibility.
	if ok, _, atErr := googleTokenInfo(ctx, "access_token", token); atErr != nil {
		return atErr
	} else if ok {
		return nil
	}
	return status.Error(codes.Unauthenticated, "invalid google token")
}

// tokenInfo is the subset of Google's tokeninfo response we care about.
// `aud` and `audience` are the same field with two spellings used by
// different versions of the endpoint; `azp` is the OAuth client ID.
type tokenInfo struct {
	Audience string `json:"audience"`
	Aud      string `json:"aud"`
	AZP      string `json:"azp"`
}

// googleTokenInfo issues GET tokeninfo?<param>=<token>. Returns (true, info,
// nil) on a 200 response, (false, _, nil) on a 4xx response (caller should
// try the other parameter), and (false, _, err) on transport-level errors.
func googleTokenInfo(ctx context.Context, param, token string) (bool, tokenInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		getTokenInfoURL()+"?"+param+"="+url.QueryEscape(token), nil)
	if err != nil {
		return false, tokenInfo{}, status.Errorf(codes.Internal, "google token request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, tokenInfo{}, status.Errorf(codes.Unauthenticated, "google token validation: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, tokenInfo{}, status.Errorf(codes.Internal, "google token read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return false, tokenInfo{}, nil
	}
	var info tokenInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return false, tokenInfo{}, status.Errorf(codes.Internal, "google token parse: %v", err)
	}
	return true, info, nil
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
