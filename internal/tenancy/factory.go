package tenancy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/petereon/embyr/internal/auth"
	"github.com/petereon/embyr/internal/config"
	"github.com/petereon/embyr/internal/registry"
	"github.com/petereon/embyr/internal/store"
	"github.com/petereon/embyr/internal/store/postgres"
	"go.uber.org/zap"
)

// ErrTenantSuspended is returned when the tenant exists but is suspended.
var ErrTenantSuspended = errors.New("tenancy: tenant is suspended")

type entry struct {
	adapter    store.StorageAdapter
	authConfig *auth.Config
}

// AdapterFactory lazily creates and caches StorageAdapters per tenant.
// Evicted entries have their connection pools closed gracefully.
type AdapterFactory struct {
	reg   registry.Client
	log   *zap.Logger
	mu    sync.Mutex
	cache *lru.Cache[string, *entry]
}

var _ io.Closer = (*AdapterFactory)(nil)

// NewAdapterFactory returns a factory with an LRU cap of capacity entries.
func NewAdapterFactory(reg registry.Client, capacity int) *AdapterFactory {
	cache, _ := lru.NewWithEvict[string, *entry](capacity, func(_ string, e *entry) {
		if e.adapter != nil {
			e.adapter.Close() //nolint:errcheck
		}
	})
	return &AdapterFactory{
		reg:   reg,
		log:   zap.NewNop(),
		cache: cache,
	}
}

// WithLogger attaches a logger to the factory.
func (f *AdapterFactory) WithLogger(log *zap.Logger) *AdapterFactory {
	f.log = log
	return f
}

func factoryCacheKey(projectID, databaseID string) string {
	return projectID + "\x00" + databaseID
}

// Get returns (adapter, authConfig, nil) for (projectID, databaseID).
// On cache miss: resolves credential, opens Postgres pool, pings, caches.
// Returns ErrTenantNotFound or ErrTenantSuspended without opening a connection.
func (f *AdapterFactory) Get(ctx context.Context, projectID, databaseID string) (store.StorageAdapter, *auth.Config, error) {
	key := factoryCacheKey(projectID, databaseID)

	f.mu.Lock()
	if e, ok := f.cache.Get(key); ok {
		f.mu.Unlock()
		return e.adapter, e.authConfig, nil
	}
	f.mu.Unlock()

	tenant, err := f.reg.Get(ctx, projectID, databaseID)
	if err != nil {
		return nil, nil, err
	}
	if tenant.Status == registry.TenantStatusSuspended {
		return nil, nil, ErrTenantSuspended
	}

	resolver, err := NewResolver(tenant)
	if err != nil {
		return nil, nil, err
	}
	dsn, err := resolver.Resolve(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("tenancy: resolve credential for %s/%s: %w", projectID, databaseID, err)
	}

	dsn, err = appendSearchPath(dsn, tenant.SchemaName)
	if err != nil {
		return nil, nil, fmt.Errorf("tenancy: append search_path: %w", err)
	}

	adapter, err := postgres.New(dsn, "", 5)
	if err != nil {
		return nil, nil, fmt.Errorf("tenancy: open postgres for %s/%s: %w", projectID, databaseID, err)
	}
	if err := adapter.Ping(ctx); err != nil {
		adapter.Close() //nolint:errcheck
		return nil, nil, fmt.Errorf("tenancy: ping %s/%s: %w", projectID, databaseID, err)
	}

	authCfg, err := buildAuthConfig(tenant)
	if err != nil {
		adapter.Close() //nolint:errcheck
		return nil, nil, fmt.Errorf("tenancy: build auth config for %s/%s: %w", projectID, databaseID, err)
	}

	e := &entry{adapter: adapter, authConfig: authCfg}
	f.mu.Lock()
	f.cache.Add(key, e)
	f.mu.Unlock()

	f.log.Info("tenancy: adapter opened", zap.String("project", projectID), zap.String("database", databaseID))
	return adapter, authCfg, nil
}

// Close drains the LRU cache, closing all pooled adapters.
func (f *AdapterFactory) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cache.Purge()
	return nil
}

// appendSearchPath adds search_path=<schema> to a Postgres DSN.
func appendSearchPath(dsn, schema string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" {
		// keyword-format DSN: "host=... search_path=..."
		return dsn + " search_path=" + schema, nil
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// buildAuthConfig constructs an auth.Config from the tenant's stored auth settings.
func buildAuthConfig(t *registry.Tenant) (*auth.Config, error) {
	authCfg := config.AuthConfig{Mode: t.AuthMode}
	if err := unmarshalAuthConfig(t.AuthConfig, &authCfg); err != nil {
		return nil, err
	}
	cfg := &config.Config{Auth: authCfg}
	return auth.New(cfg, zap.NewNop())
}

// unmarshalAuthConfig populates fields from raw JSON.
func unmarshalAuthConfig(raw []byte, out *config.AuthConfig) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal(raw, &m); err != nil {
		return fmt.Errorf("tenancy: parse auth_config: %w", err)
	}
	if v, ok := m["key"]; ok {
		out.Key = v
	}
	if v, ok := m["google_project_id"]; ok {
		out.GoogleProjectID = v
	}
	if v, ok := m["mtls_ca"]; ok {
		out.MTLSCACert = v
	}
	return nil
}
