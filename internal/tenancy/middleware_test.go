package tenancy_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/petereon/embyr/internal/auth"
	"github.com/petereon/embyr/internal/config"
	"github.com/petereon/embyr/internal/registry"
	"github.com/petereon/embyr/internal/store"
	"github.com/petereon/embyr/internal/tenancy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestParseTenantKey(t *testing.T) {
	cases := []struct {
		path     string
		project  string
		database string
		ok       bool
	}{
		{"projects/acme/databases/prod/documents/users/alice", "acme", "prod", true},
		{"projects/acme/databases/prod", "acme", "prod", true},
		{"projects/acme/databases/prod/documents", "acme", "prod", true},
		{"", "", "", false},
		{"projects/acme", "", "", false},
		{"notapath", "", "", false},
		// Real Firestore REST URL forms — these all fail before the fix.
		{"/v1/projects/acme/databases/prod/documents/users/alice", "acme", "prod", true},
		{"/v1/projects/acme/databases/prod", "acme", "prod", true},
		{"/v1/projects/acme/databases/prod/documents:batchGet", "acme", "prod", true},
		{"v1/projects/acme/databases/prod", "acme", "prod", true},
		// Empty segments still rejected.
		{"/v1/projects//databases/prod", "", "", false},
		{"/v1/projects/acme/databases/", "", "", false},
	}
	for _, tc := range cases {
		p, d, ok := tenancy.ParseTenantKey(tc.path)
		assert.Equal(t, tc.ok, ok, "path=%q", tc.path)
		assert.Equal(t, tc.project, p, "path=%q", tc.path)
		assert.Equal(t, tc.database, d, "path=%q", tc.path)
	}
}

// stubFactory implements tenancy.FactoryLike for middleware tests.
type stubFactory struct {
	authConfig    *auth.Config
	rawAuthConfig []byte
	errToReturn   error
}

func (f *stubFactory) Get(_ context.Context, _, _ string) (store.StorageAdapter, *auth.Config, []byte, error) {
	if f.errToReturn != nil {
		return nil, nil, nil, f.errToReturn
	}
	return nil, f.authConfig, f.rawAuthConfig, nil
}

// newKeyStub returns a stubFactory pre-configured for "key" auth mode.
func newKeyStub(t *testing.T, key string) *stubFactory {
	t.Helper()
	rawCfg, err := json.Marshal(map[string]string{"key": key})
	require.NoError(t, err)
	authCfg, err := auth.New(
		&config.Config{Auth: config.AuthConfig{Mode: "key", Key: key}},
		zap.NewNop(),
	)
	require.NoError(t, err)
	return &stubFactory{authConfig: authCfg, rawAuthConfig: rawCfg}
}

func okHandler(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }

func TestHTTPMiddleware_RejectsUnparsablePath(t *testing.T) {
	factory := newKeyStub(t, "secret")
	handler := tenancy.HTTPMiddleware(factory, zap.NewNop(), http.HandlerFunc(okHandler))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestHTTPMiddleware_RejectsMissingAuth(t *testing.T) {
	factory := newKeyStub(t, "secret")
	handler := tenancy.HTTPMiddleware(factory, zap.NewNop(), http.HandlerFunc(okHandler))

	req := httptest.NewRequest(http.MethodGet,
		"/v1/projects/proj/databases/db/documents/users/alice", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestHTTPMiddleware_RejectsWrongKey(t *testing.T) {
	factory := newKeyStub(t, "correct-key")
	handler := tenancy.HTTPMiddleware(factory, zap.NewNop(), http.HandlerFunc(okHandler))

	req := httptest.NewRequest(http.MethodGet,
		"/v1/projects/proj/databases/db/documents/users/alice", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestHTTPMiddleware_AcceptsCorrectKey(t *testing.T) {
	factory := newKeyStub(t, "correct-key")
	handler := tenancy.HTTPMiddleware(factory, zap.NewNop(), http.HandlerFunc(okHandler))

	req := httptest.NewRequest(http.MethodGet,
		"/v1/projects/proj/databases/db/documents/users/alice", nil)
	req.Header.Set("Authorization", "Bearer correct-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHTTPMiddleware_RejectsUnknownTenant(t *testing.T) {
	factory := &stubFactory{errToReturn: registry.ErrTenantNotFound}
	handler := tenancy.HTTPMiddleware(factory, zap.NewNop(), http.HandlerFunc(okHandler))

	req := httptest.NewRequest(http.MethodGet,
		"/v1/projects/ghost/databases/db/documents/users/alice", nil)
	req.Header.Set("Authorization", "Bearer any-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}
