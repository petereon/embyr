package auth_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/petereon/embyr/internal/auth"
	"github.com/petereon/embyr/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func nopHandler(ctx context.Context, req interface{}) (interface{}, error) { return "ok", nil }

func ctxWithBearer(token string) context.Context {
	md := metadata.Pairs("authorization", "Bearer "+token)
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestNoneMode_PassesAll(t *testing.T) {
	cfg := &config.Config{}
	cfg.Auth.Mode = "none"
	a, err := auth.New(cfg, zap.NewNop())
	require.NoError(t, err)

	_, err = a.Unary(context.Background(), nil, &grpc.UnaryServerInfo{}, nopHandler)
	assert.NoError(t, err)
}

func TestKeyMode_ValidKey(t *testing.T) {
	cfg := &config.Config{}
	cfg.Auth.Mode = "key"
	cfg.Auth.Key = "secret"
	a, err := auth.New(cfg, zap.NewNop())
	require.NoError(t, err)

	ctx := ctxWithBearer("secret")
	_, err = a.Unary(ctx, nil, &grpc.UnaryServerInfo{}, nopHandler)
	assert.NoError(t, err)
}

func TestKeyMode_WrongKey(t *testing.T) {
	cfg := &config.Config{}
	cfg.Auth.Mode = "key"
	cfg.Auth.Key = "secret"
	a, err := auth.New(cfg, zap.NewNop())
	require.NoError(t, err)

	ctx := ctxWithBearer("wrong")
	_, err = a.Unary(ctx, nil, &grpc.UnaryServerInfo{}, nopHandler)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestKeyMode_MissingHeader(t *testing.T) {
	cfg := &config.Config{}
	cfg.Auth.Mode = "key"
	cfg.Auth.Key = "secret"
	a, err := auth.New(cfg, zap.NewNop())
	require.NoError(t, err)

	_, err = a.Unary(context.Background(), nil, &grpc.UnaryServerInfo{}, nopHandler)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestKeyMode_EmptyKey_ConfigError(t *testing.T) {
	cfg := &config.Config{}
	cfg.Auth.Mode = "key"
	cfg.Auth.Key = ""
	_, err := auth.New(cfg, zap.NewNop())
	require.Error(t, err)
}

func TestGoogleMode_ValidToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"audience": "my-project",
		})
	}))
	defer srv.Close()

	auth.SetTokenInfoURL(srv.URL)
	defer auth.SetTokenInfoURL("https://www.googleapis.com/oauth2/v1/tokeninfo")

	cfg := &config.Config{}
	cfg.Auth.Mode = "google"
	cfg.Auth.GoogleProjectID = "my-project"
	a, err := auth.New(cfg, zap.NewNop())
	require.NoError(t, err)

	ctx := ctxWithBearer("fake-token")
	_, err = a.Unary(ctx, nil, &grpc.UnaryServerInfo{}, nopHandler)
	assert.NoError(t, err)
}

func TestGoogleMode_WrongAudience(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"audience": "different-project",
		})
	}))
	defer srv.Close()

	auth.SetTokenInfoURL(srv.URL)
	defer auth.SetTokenInfoURL("https://www.googleapis.com/oauth2/v1/tokeninfo")

	cfg := &config.Config{}
	cfg.Auth.Mode = "google"
	cfg.Auth.GoogleProjectID = "my-project"
	a, err := auth.New(cfg, zap.NewNop())
	require.NoError(t, err)

	ctx := ctxWithBearer("fake-token")
	_, err = a.Unary(ctx, nil, &grpc.UnaryServerInfo{}, nopHandler)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestNoneMode_EmptyMode_PassesAll(t *testing.T) {
	cfg := &config.Config{}
	cfg.Auth.Mode = "" // empty string should behave as "none"
	a, err := auth.New(cfg, zap.NewNop())
	require.NoError(t, err)

	_, err = a.Unary(context.Background(), nil, &grpc.UnaryServerInfo{}, nopHandler)
	assert.NoError(t, err)
}

func TestUnknownMode_ConfigError(t *testing.T) {
	cfg := &config.Config{}
	cfg.Auth.Mode = "unsupported"
	_, err := auth.New(cfg, zap.NewNop())
	require.Error(t, err)
}

func TestGoogleMode_EmptyProjectID_ConfigError(t *testing.T) {
	cfg := &config.Config{}
	cfg.Auth.Mode = "google"
	cfg.Auth.GoogleProjectID = ""
	_, err := auth.New(cfg, zap.NewNop())
	require.Error(t, err)
}

func TestGoogleMode_AZPMatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// audience is different but azp matches
		json.NewEncoder(w).Encode(map[string]string{
			"audience": "something-else",
			"azp":      "my-project",
		})
	}))
	defer srv.Close()

	auth.SetTokenInfoURL(srv.URL)
	defer auth.SetTokenInfoURL("https://www.googleapis.com/oauth2/v1/tokeninfo")

	cfg := &config.Config{}
	cfg.Auth.Mode = "google"
	cfg.Auth.GoogleProjectID = "my-project"
	a, err := auth.New(cfg, zap.NewNop())
	require.NoError(t, err)

	ctx := ctxWithBearer("fake-token")
	_, err = a.Unary(ctx, nil, &grpc.UnaryServerInfo{}, nopHandler)
	assert.NoError(t, err)
}

func TestKeyMode_Stream_ValidKey(t *testing.T) {
	cfg := &config.Config{}
	cfg.Auth.Mode = "key"
	cfg.Auth.Key = "stream-secret"
	a, err := auth.New(cfg, zap.NewNop())
	require.NoError(t, err)

	ctx := ctxWithBearer("stream-secret")
	mockStream := &mockServerStream{ctx: ctx}
	err = a.Stream(nil, mockStream, &grpc.StreamServerInfo{}, func(srv interface{}, stream grpc.ServerStream) error {
		return nil
	})
	assert.NoError(t, err)
}

func TestKeyMode_Stream_WrongKey(t *testing.T) {
	cfg := &config.Config{}
	cfg.Auth.Mode = "key"
	cfg.Auth.Key = "stream-secret"
	a, err := auth.New(cfg, zap.NewNop())
	require.NoError(t, err)

	ctx := ctxWithBearer("wrong-key")
	mockStream := &mockServerStream{ctx: ctx}
	err = a.Stream(nil, mockStream, &grpc.StreamServerInfo{}, func(srv interface{}, stream grpc.ServerStream) error {
		return nil
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

// mockServerStream implements grpc.ServerStream for testing.
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context { return m.ctx }
