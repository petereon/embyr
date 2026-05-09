package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/petereon/embyr/internal/auth"
	"github.com/petereon/embyr/internal/config"
	"github.com/petereon/embyr/internal/registry"
	"github.com/petereon/embyr/internal/server"
	"github.com/petereon/embyr/internal/store"
	"github.com/petereon/embyr/internal/store/sqlite"
	"github.com/petereon/embyr/internal/tenancy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// mtFactory is an in-memory FactoryLike backed by named sqlite tenants.
type mtFactory struct {
	entries map[string]*mtEntry
}

type mtEntry struct {
	adapter    store.StorageAdapter
	authConfig *auth.Config
	rawCfg     []byte
}

func (f *mtFactory) Get(_ context.Context, projectID, databaseID string) (store.StorageAdapter, *auth.Config, []byte, error) {
	key := projectID + "/" + databaseID
	e, ok := f.entries[key]
	if !ok {
		return nil, nil, nil, registry.ErrTenantNotFound
	}
	return e.adapter, e.authConfig, e.rawCfg, nil
}

func newKeyEntry(t *testing.T, key string) *mtEntry {
	t.Helper()
	dir := t.TempDir()
	adapter, err := sqlite.New(dir+"/test.db", "../../migrations/sqlite")
	require.NoError(t, err)
	require.NoError(t, adapter.Migrate(context.Background()))
	t.Cleanup(func() { adapter.Close() }) //nolint:errcheck

	authCfg, err := auth.New(
		&config.Config{Auth: config.AuthConfig{Mode: "key", Key: key}},
		zap.NewNop(),
	)
	require.NoError(t, err)

	rawCfg, _ := json.Marshal(map[string]string{"key": key})
	return &mtEntry{adapter: adapter, authConfig: authCfg, rawCfg: rawCfg}
}

func startMultiTenantServer(t *testing.T, factory tenancy.FactoryLike) string {
	t.Helper()
	grpcPort := freePort(t)
	restPort := freePort(t)

	cfg := &config.Config{}
	cfg.Server.GRPCPort = grpcPort
	cfg.Server.RESTPort = restPort
	cfg.Transactions.TTL = 60 * time.Second
	cfg.Transactions.SweepInterval = 30 * time.Second

	srv, err := server.NewMultiTenant(cfg, factory, zap.NewNop())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go srv.Run(ctx) //nolint:errcheck

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", restPort)
	require.Eventually(t, func() bool {
		resp, err := http.Get(baseURL + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 50*time.Millisecond, "server did not become ready")

	return baseURL
}

func TestMultiTenant_AuthMatrix(t *testing.T) {
	factory := &mtFactory{entries: map[string]*mtEntry{
		"proj-a/db-a": newKeyEntry(t, "key-for-a"),
		"proj-b/db-b": newKeyEntry(t, "key-for-b"),
	}}
	base := startMultiTenantServer(t, factory)

	pathA := base + "/v1/projects/proj-a/databases/db-a/documents/col/doc"
	pathB := base + "/v1/projects/proj-b/databases/db-b/documents/col/doc"

	cases := []struct {
		name     string
		path     string
		token    string
		wantCode int
	}{
		{"no_token_tenant_a", pathA, "", http.StatusUnauthorized},
		{"wrong_key_tenant_a", pathA, "Bearer wrong-key", http.StatusUnauthorized},
		// Gateway routes .../documents/col/doc to ListDocuments (empty list → 200),
		// not GetDocument (would return 404). Key assertion: auth passed, backend reached.
		{"correct_key_tenant_a", pathA, "Bearer key-for-a", http.StatusOK},
		{"no_token_tenant_b", pathB, "", http.StatusUnauthorized},
		{"wrong_key_tenant_b", pathB, "Bearer wrong-key", http.StatusUnauthorized},
		{"cross_tenant_key_a_path_b", pathB, "Bearer key-for-a", http.StatusUnauthorized},
		{"cross_tenant_key_b_path_a", pathA, "Bearer key-for-b", http.StatusUnauthorized},
		{"correct_key_tenant_b", pathB, "Bearer key-for-b", http.StatusOK}, // same gateway routing as above
		{"unknown_tenant", base + "/v1/projects/ghost/databases/db/documents/col/doc", "Bearer any-key", http.StatusUnauthorized},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, tc.path, nil)
			require.NoError(t, err)
			if tc.token != "" {
				req.Header.Set("Authorization", tc.token)
			}
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			io.Copy(io.Discard, resp.Body) //nolint:errcheck
			resp.Body.Close()
			assert.Equal(t, tc.wantCode, resp.StatusCode, "case %s", tc.name)
		})
	}
}

func TestMultiTenant_HealthzRequiresNoAuth(t *testing.T) {
	factory := &mtFactory{entries: map[string]*mtEntry{}}
	base := startMultiTenantServer(t, factory)

	resp, err := http.Get(base + "/healthz")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
