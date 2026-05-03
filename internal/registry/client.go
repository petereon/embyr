package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrTenantNotFound is returned when no active tenant matches (projectID, databaseID).
var ErrTenantNotFound = errors.New("registry: tenant not found")

// Client is the data-plane interface for tenant lookups.
type Client interface {
	// Get returns the active tenant for (projectID, databaseID).
	// Returns ErrTenantNotFound if absent or suspended.
	Get(ctx context.Context, projectID, databaseID string) (*Tenant, error)
	Close()
}

type cacheEntry struct {
	tenant    *Tenant
	expiresAt time.Time
}

// PostgresClient reads tenants from Embyr's registry DB with a TTL cache.
type PostgresClient struct {
	db  *sql.DB
	ttl time.Duration

	mu    sync.RWMutex
	cache map[string]*cacheEntry // key: projectID+"/"+databaseID
}

// NewPostgresClient returns a Client backed by db with the given cache TTL.
func NewPostgresClient(db *sql.DB, ttl time.Duration) *PostgresClient {
	return &PostgresClient{db: db, ttl: ttl, cache: make(map[string]*cacheEntry)}
}

func cacheKey(projectID, databaseID string) string {
	return projectID + "/" + databaseID
}

// Get returns a cached tenant or fetches from the registry DB.
func (c *PostgresClient) Get(ctx context.Context, projectID, databaseID string) (*Tenant, error) {
	key := cacheKey(projectID, databaseID)

	c.mu.RLock()
	entry, ok := c.cache[key]
	c.mu.RUnlock()
	if ok && time.Now().Before(entry.expiresAt) {
		if entry.tenant == nil {
			return nil, ErrTenantNotFound
		}
		return entry.tenant, nil
	}

	t, err := c.fetch(ctx, projectID, databaseID)
	notFound := errors.Is(err, ErrTenantNotFound)
	if err != nil && !notFound {
		return nil, err
	}

	c.mu.Lock()
	if notFound {
		c.cache[key] = &cacheEntry{tenant: nil, expiresAt: time.Now().Add(c.ttl)}
	} else {
		c.cache[key] = &cacheEntry{tenant: t, expiresAt: time.Now().Add(c.ttl)}
	}
	c.mu.Unlock()

	if notFound {
		return nil, ErrTenantNotFound
	}
	return t, nil
}

func (c *PostgresClient) fetch(ctx context.Context, projectID, databaseID string) (*Tenant, error) {
	row := c.db.QueryRowContext(ctx, `
		SELECT id, project_id, database_id, schema_name, credential_type,
		       credential_ref, auth_mode, auth_config, status, updated_at
		FROM tenants
		WHERE project_id = $1 AND database_id = $2
		LIMIT 1`, projectID, databaseID)

	var t Tenant
	var authConfigBytes []byte
	err := row.Scan(
		&t.ID, &t.ProjectID, &t.DatabaseID, &t.SchemaName,
		&t.CredentialType, &t.CredentialRef,
		&t.AuthMode, &authConfigBytes, &t.Status, &t.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrTenantNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("registry: fetch tenant: %w", err)
	}
	if t.Status == TenantStatusSuspended {
		return nil, ErrTenantNotFound
	}
	t.AuthConfig = json.RawMessage(authConfigBytes)
	return &t, nil
}

// Close is a no-op; the caller owns the *sql.DB lifetime.
func (c *PostgresClient) Close() {}
