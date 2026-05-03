# Core Tenancy Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add multi-tenant routing to the data plane so each Firestore request is served by the customer's own Postgres DB, while keeping single-tenant mode and all existing tests unchanged.

**Architecture:** A `registry.Client` reads tenant config from Embyr's own Neon Postgres (60s TTL cache). An `AdapterFactory` lazily opens a `StorageAdapter` per `(projectId, databaseId)` pair using an LRU cache (cap 200). gRPC/HTTP middleware extracts the tenant key from each request path, resolves the adapter, validates the token against the tenant's auth config, and injects the adapter into the request context. Handlers call `s.adapter(ctx)` instead of `s.db` directly; the helper falls back to `s.db` in single-tenant mode so no existing tests break.

**Tech Stack:** Go 1.25, `github.com/hashicorp/golang-lru/v2`, `cloud.google.com/go/secretmanager/apiv1`, `github.com/aws/aws-sdk-go-v2`, `github.com/golang-migrate/migrate/v4` (already present), `jackc/pgx/v5` (already present)

---

## File Map

**New files:**
- `internal/registry/tenant.go` — `Tenant` struct, `CredentialType`/`TenantStatus` constants
- `internal/registry/client.go` — `Client` interface + `PostgresClient` (60s TTL cache)
- `internal/registry/client_test.go`
- `internal/migrations/runner.go` — `Runner`: schema-aware wrapper around golang-migrate
- `internal/migrations/runner_test.go`
- `internal/tenancy/context.go` — `WithAdapter`/`AdapterFromCtx`, `WithAuthInfo`/`AuthInfoFromCtx`
- `internal/tenancy/resolver.go` — `CredentialResolver` interface + `NewResolver` factory
- `internal/tenancy/gcp.go` — `GCPSecretResolver`
- `internal/tenancy/aws.go` — `AWSSecretResolver`
- `internal/tenancy/embyr_secret.go` — `EmbyrSecretResolver`
- `internal/tenancy/agent.go` — `AgentResolver` (stub, returns error)
- `internal/tenancy/factory.go` — `AdapterFactory` with LRU cache
- `internal/tenancy/factory_test.go`
- `internal/tenancy/middleware.go` — gRPC interceptors + HTTP middleware + path extraction
- `internal/tenancy/middleware_test.go`
- `cmd/data/main.go` — multi-tenant entry point

**Modified files:**
- `go.mod` / `go.sum` — add golang-lru/v2, secretmanager, aws-sdk-v2
- `internal/auth/interceptor.go` — add `ValidateForTenant(ctx, mode, rawConfig)`
- `internal/auth/interceptor_test.go` — tests for `ValidateForTenant`
- `internal/server/server.go` — add `NewMultiTenant`, update `firestoreServer` struct
- `internal/server/handlers.go` — add `adapter()` method, replace all `s.db.` with `s.adapter(ctx).`
- `internal/server/aggregation.go` — replace `s.db.` with `s.adapter(ctx).`
- `internal/server/transactions.go` — replace `s.db.` with `s.adapter(ctx).`

**Unchanged (zero edits):**
- `cmd/embyr/main.go` — single-tenant entry point stays intact
- `internal/store/` — all store packages
- `internal/webchannel/`, `internal/listen/`, `internal/codec/`
- `internal/server/listen.go` — does not use `s.db`
- All migration SQL files in `migrations/`

---

## Task 1: Add dependencies to go.mod

**Files:**
- Modify: `go.mod`

- [ ] **Step 1: Add LRU, GCP Secret Manager, AWS SDK**

```bash
cd /path/to/embyr
go get github.com/hashicorp/golang-lru/v2@latest
go get cloud.google.com/go/secretmanager/apiv1@latest
go get google.golang.org/genproto/googleapis/cloud/secretmanager/v1@latest
go get github.com/aws/aws-sdk-go-v2@latest
go get github.com/aws/aws-sdk-go-v2/config@latest
go get github.com/aws/aws-sdk-go-v2/service/secretsmanager@latest
go get cloud.google.com/go/kms/apiv1@latest
go mod tidy
```

- [ ] **Step 2: Verify build still passes**

```bash
go build ./...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: add tenancy layer dependencies"
```

---

## Task 2: internal/registry — Tenant type

**Files:**
- Create: `internal/registry/tenant.go`

- [ ] **Step 1: Write the file**

```go
package registry

import (
	"encoding/json"
	"time"
)

// CredentialType identifies how the data plane resolves a DSN for a tenant.
type CredentialType string

const (
	CredentialGCPSecret   CredentialType = "gcp_secret"
	CredentialAWSSecret   CredentialType = "aws_secret"
	CredentialEmbyrSecret CredentialType = "embyr_secret"
	CredentialAgent       CredentialType = "agent"
)

// TenantStatus reflects the lifecycle state stored in the registry.
type TenantStatus string

const (
	TenantStatusProvisioning TenantStatus = "provisioning"
	TenantStatusActive       TenantStatus = "active"
	TenantStatusSuspended    TenantStatus = "suspended"
)

// Tenant is the data-plane view of a registered tenant.
// Fields map 1:1 to the tenants table columns needed by the data plane.
type Tenant struct {
	ID             string
	ProjectID      string
	DatabaseID     string
	SchemaName     string
	CredentialType CredentialType
	CredentialRef  string
	AuthMode       string
	AuthConfig     json.RawMessage
	Status         TenantStatus
	UpdatedAt      time.Time
}
```

- [ ] **Step 2: Compile check**

```bash
go build ./internal/registry/...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/registry/tenant.go
git commit -m "feat(registry): tenant type and credential/status constants"
```

---

## Task 3: internal/registry — PostgresClient

**Files:**
- Create: `internal/registry/client.go`
- Create: `internal/registry/client_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/registry/client_test.go
package registry_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/petereon/embyr/internal/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_REGISTRY_DSN")
	if dsn == "" {
		t.Skip("TEST_REGISTRY_DSN not set")
	}
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func seedTenant(t *testing.T, db *sql.DB, tenant registry.Tenant) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
		INSERT INTO tenants
			(id, project_id, database_id, schema_name, credential_type,
			 credential_ref, auth_mode, auth_config, status, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now(),now())
		ON CONFLICT (project_id, database_id) DO UPDATE
			SET status = EXCLUDED.status, updated_at = now()`,
		tenant.ID, tenant.ProjectID, tenant.DatabaseID, tenant.SchemaName,
		string(tenant.CredentialType), tenant.CredentialRef,
		tenant.AuthMode, []byte(tenant.AuthConfig), string(tenant.Status))
	require.NoError(t, err)
	t.Cleanup(func() {
		db.ExecContext(context.Background(), `DELETE FROM tenants WHERE id = $1`, tenant.ID)
	})
}

func TestPostgresClient_Get(t *testing.T) {
	db := openTestDB(t)
	want := registry.Tenant{
		ID:             "test-acme-prod",
		ProjectID:      "acme",
		DatabaseID:     "prod",
		SchemaName:     "embyr_prod",
		CredentialType: registry.CredentialGCPSecret,
		CredentialRef:  "projects/acme/secrets/dsn/versions/latest",
		AuthMode:       "google",
		AuthConfig:     json.RawMessage(`{"google_project_id":"acme"}`),
		Status:         registry.TenantStatusActive,
	}
	seedTenant(t, db, want)

	client := registry.NewPostgresClient(db, 60*time.Second)
	defer client.Close()

	got, err := client.Get(context.Background(), "acme", "prod")
	require.NoError(t, err)
	assert.Equal(t, want.ID, got.ID)
	assert.Equal(t, want.CredentialType, got.CredentialType)
	assert.Equal(t, want.Status, got.Status)
}

func TestPostgresClient_Get_NotFound(t *testing.T) {
	db := openTestDB(t)
	client := registry.NewPostgresClient(db, 60*time.Second)
	defer client.Close()

	_, err := client.Get(context.Background(), "nope", "nope")
	assert.ErrorIs(t, err, registry.ErrTenantNotFound)
}

func TestPostgresClient_Get_CachesTTL(t *testing.T) {
	db := openTestDB(t)
	want := registry.Tenant{
		ID: "test-cache", ProjectID: "cache", DatabaseID: "db",
		SchemaName: "embyr_db", CredentialType: registry.CredentialEmbyrSecret,
		CredentialRef: "blob", AuthMode: "none",
		AuthConfig: json.RawMessage(`{}`), Status: registry.TenantStatusActive,
	}
	seedTenant(t, db, want)

	client := registry.NewPostgresClient(db, 100*time.Millisecond)
	defer client.Close()

	// First call hits DB.
	_, err := client.Get(context.Background(), "cache", "db")
	require.NoError(t, err)

	// Delete from DB — cached result should still return.
	db.ExecContext(context.Background(), `DELETE FROM tenants WHERE id = $1`, want.ID)
	got, err := client.Get(context.Background(), "cache", "db")
	require.NoError(t, err)
	assert.Equal(t, want.ID, got.ID)

	// After TTL, returns not found.
	time.Sleep(150 * time.Millisecond)
	_, err = client.Get(context.Background(), "cache", "db")
	assert.ErrorIs(t, err, registry.ErrTenantNotFound)
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./internal/registry/... 2>&1 | head -20
```

Expected: compile error — `registry.NewPostgresClient`, `registry.ErrTenantNotFound` undefined.

- [ ] **Step 3: Write the implementation**

```go
// internal/registry/client.go
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
```

- [ ] **Step 4: Run test to verify it passes**

```bash
TEST_REGISTRY_DSN="postgres://user:pass@localhost/embyr_registry?sslmode=disable" \
  go test ./internal/registry/... -v -run TestPostgresClient
```

Expected: all three tests PASS (or SKIP if `TEST_REGISTRY_DSN` unset).

- [ ] **Step 5: Commit**

```bash
git add internal/registry/
git commit -m "feat(registry): PostgresClient with TTL cache"
```

---

## Task 4: internal/migrations — schema-aware Runner

**Files:**
- Create: `internal/migrations/runner.go`
- Create: `internal/migrations/runner_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/migrations/runner_test.go
package migrations_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/petereon/embyr/internal/migrations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set")
	}
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestRunner_UpDown(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := "embyr_migration_test"
	r := migrations.NewRunner("../../migrations/postgres")

	t.Cleanup(func() {
		db.ExecContext(ctx, `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
	})

	// Up: schema + tables should be created.
	require.NoError(t, r.Up(ctx, db, schema))

	var exists bool
	err := db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables
		 WHERE table_schema=$1 AND table_name='documents')`, schema).Scan(&exists)
	require.NoError(t, err)
	assert.True(t, exists, "documents table should exist after Up")

	// Down: schema should be dropped.
	require.NoError(t, r.Down(ctx, db, schema))
	err = db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name=$1)`,
		schema).Scan(&exists)
	require.NoError(t, err)
	assert.False(t, exists, "schema should be gone after Down")
}

func TestRunner_UpIdempotent(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := "embyr_idempotent_test"
	r := migrations.NewRunner("../../migrations/postgres")

	t.Cleanup(func() {
		db.ExecContext(ctx, `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
	})

	require.NoError(t, r.Up(ctx, db, schema))
	// Running Up a second time must not error (ErrNoChange is swallowed).
	require.NoError(t, r.Up(ctx, db, schema))
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./internal/migrations/... 2>&1 | head -10
```

Expected: compile error — `migrations.NewRunner` undefined.

- [ ] **Step 3: Write the implementation**

```go
// internal/migrations/runner.go
package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"

	"github.com/golang-migrate/migrate/v4"
	migratepostgres "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// validSchema matches safe schema identifiers to prevent SQL injection.
var validSchema = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// Runner applies schema-namespaced migrations to a customer's Postgres DB.
// It uses golang-migrate with the schema name set so that all tables and the
// migration tracking table land in the target schema, not public.
type Runner struct {
	migrationsPath string // e.g. "migrations/postgres"
}

// NewRunner returns a Runner that reads SQL files from migrationsPath.
func NewRunner(migrationsPath string) *Runner {
	return &Runner{migrationsPath: migrationsPath}
}

// Up creates the schema if absent, then applies all pending migrations.
func (r *Runner) Up(ctx context.Context, db *sql.DB, schemaName string) error {
	if !validSchema.MatchString(schemaName) {
		return fmt.Errorf("migrations: invalid schema name %q", schemaName)
	}

	if _, err := db.ExecContext(ctx,
		`CREATE SCHEMA IF NOT EXISTS "`+schemaName+`"`); err != nil {
		return fmt.Errorf("migrations: create schema %q: %w", schemaName, err)
	}

	m, err := r.newMigrate(db, schemaName)
	if err != nil {
		return err
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrations: up %q: %w", schemaName, err)
	}
	return nil
}

// Down drops all tables by dropping the schema entirely.
func (r *Runner) Down(ctx context.Context, db *sql.DB, schemaName string) error {
	if !validSchema.MatchString(schemaName) {
		return fmt.Errorf("migrations: invalid schema name %q", schemaName)
	}
	if _, err := db.ExecContext(ctx,
		`DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`); err != nil {
		return fmt.Errorf("migrations: drop schema %q: %w", schemaName, err)
	}
	return nil
}

func (r *Runner) newMigrate(db *sql.DB, schemaName string) (*migrate.Migrate, error) {
	driver, err := migratepostgres.WithInstance(db, &migratepostgres.Config{
		SchemaName:      schemaName,
		MigrationsTable: "schema_migrations",
	})
	if err != nil {
		return nil, fmt.Errorf("migrations: driver for %q: %w", schemaName, err)
	}
	m, err := migrate.NewWithDatabaseInstance(
		"file://"+r.migrationsPath, "postgres", driver)
	if err != nil {
		return nil, fmt.Errorf("migrations: init for %q: %w", schemaName, err)
	}
	return m, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
TEST_POSTGRES_DSN="postgres://user:pass@localhost/embyrtest?sslmode=disable" \
  go test ./internal/migrations/... -v
```

Expected: PASS (or SKIP if env var unset).

- [ ] **Step 5: Commit**

```bash
git add internal/migrations/
git commit -m "feat(migrations): schema-aware runner wrapping golang-migrate"
```

---

## Task 5: internal/tenancy — context helpers

**Files:**
- Create: `internal/tenancy/context.go`

- [ ] **Step 1: Write the file**

```go
// internal/tenancy/context.go
package tenancy

import (
	"context"

	"github.com/petereon/embyr/internal/store"
)

type ctxAdapterKey struct{}

// WithAdapter stores a resolved StorageAdapter in the context.
func WithAdapter(ctx context.Context, a store.StorageAdapter) context.Context {
	return context.WithValue(ctx, ctxAdapterKey{}, a)
}

// AdapterFromCtx retrieves the StorageAdapter injected by tenancy middleware.
// Returns nil if none was injected (single-tenant mode).
func AdapterFromCtx(ctx context.Context) store.StorageAdapter {
	a, _ := ctx.Value(ctxAdapterKey{}).(store.StorageAdapter)
	return a
}

// AuthInfo carries the resolved tenant auth config for the current request.
type AuthInfo struct {
	Mode   string
	Config []byte // raw JSON auth config
}

type ctxAuthKey struct{}

// WithAuthInfo stores AuthInfo in the context.
func WithAuthInfo(ctx context.Context, info AuthInfo) context.Context {
	return context.WithValue(ctx, ctxAuthKey{}, info)
}

// AuthInfoFromCtx retrieves AuthInfo from the context.
func AuthInfoFromCtx(ctx context.Context) (AuthInfo, bool) {
	info, ok := ctx.Value(ctxAuthKey{}).(AuthInfo)
	return info, ok
}
```

- [ ] **Step 2: Compile check**

```bash
go build ./internal/tenancy/...
```

- [ ] **Step 3: Commit**

```bash
git add internal/tenancy/context.go
git commit -m "feat(tenancy): context helpers for adapter and auth injection"
```

---

## Task 6: internal/tenancy — CredentialResolver interface and implementations

**Files:**
- Create: `internal/tenancy/resolver.go`
- Create: `internal/tenancy/gcp.go`
- Create: `internal/tenancy/aws.go`
- Create: `internal/tenancy/embyr_secret.go`
- Create: `internal/tenancy/agent.go`

- [ ] **Step 1: Write resolver.go — interface and factory**

```go
// internal/tenancy/resolver.go
package tenancy

import (
	"context"
	"fmt"

	"github.com/petereon/embyr/internal/registry"
)

// CredentialResolver resolves a Postgres DSN for a tenant.
type CredentialResolver interface {
	Resolve(ctx context.Context) (dsn string, err error)
}

// NewResolver returns the correct CredentialResolver for the tenant's credential type.
func NewResolver(t *registry.Tenant) (CredentialResolver, error) {
	switch t.CredentialType {
	case registry.CredentialGCPSecret:
		return &GCPSecretResolver{resourceName: t.CredentialRef}, nil
	case registry.CredentialAWSSecret:
		return &AWSSecretResolver{secretARN: t.CredentialRef}, nil
	case registry.CredentialEmbyrSecret:
		return &EmbyrSecretResolver{encryptedDSN: t.CredentialRef}, nil
	case registry.CredentialAgent:
		return &AgentResolver{agentID: t.CredentialRef}, nil
	default:
		return nil, fmt.Errorf("tenancy: unknown credential type %q", t.CredentialType)
	}
}
```

- [ ] **Step 2: Write gcp.go**

```go
// internal/tenancy/gcp.go
package tenancy

import (
	"context"
	"fmt"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
)

// GCPSecretResolver fetches a DSN from GCP Secret Manager.
// The customer stores their DSN in their own project and grants
// Embyr's service account secretmanager.secretAccessor on that secret.
type GCPSecretResolver struct {
	resourceName string // e.g. "projects/acme/secrets/embyr-dsn/versions/latest"
}

func (r *GCPSecretResolver) Resolve(ctx context.Context) (string, error) {
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return "", fmt.Errorf("tenancy: gcp secret manager client: %w", err)
	}
	defer client.Close()

	resp, err := client.AccessSecretVersion(ctx,
		&secretmanagerpb.AccessSecretVersionRequest{Name: r.resourceName})
	if err != nil {
		return "", fmt.Errorf("tenancy: access gcp secret %q: %w", r.resourceName, err)
	}
	return string(resp.Payload.Data), nil
}
```

- [ ] **Step 3: Write aws.go**

```go
// internal/tenancy/aws.go
package tenancy

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

// AWSSecretResolver fetches a DSN from AWS Secrets Manager.
// The customer stores their DSN in their own account and grants
// Embyr's IAM role secretsmanager:GetSecretValue on that secret.
type AWSSecretResolver struct {
	secretARN string // e.g. "arn:aws:secretsmanager:us-east-1:123:secret:embyr-dsn"
}

func (r *AWSSecretResolver) Resolve(ctx context.Context) (string, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", fmt.Errorf("tenancy: aws config: %w", err)
	}
	client := secretsmanager.NewFromConfig(cfg)
	out, err := client.GetSecretValue(ctx,
		&secretsmanager.GetSecretValueInput{SecretId: &r.secretARN})
	if err != nil {
		return "", fmt.Errorf("tenancy: get aws secret %q: %w", r.secretARN, err)
	}
	if out.SecretString == nil {
		return "", fmt.Errorf("tenancy: aws secret %q has no string value", r.secretARN)
	}
	return *out.SecretString, nil
}
```

- [ ] **Step 4: Write embyr_secret.go**

```go
// internal/tenancy/embyr_secret.go
package tenancy

import (
	"context"
	"fmt"

	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
)

// EmbyrSecretResolver decrypts a DSN blob stored in the registry using
// Embyr's own GCP Cloud KMS key. The credential_ref field holds the
// KMS-encrypted ciphertext (base64-encoded) prefixed with the key resource
// name, separated by "|": "<kms-key-resource>|<base64-ciphertext>".
type EmbyrSecretResolver struct {
	encryptedDSN string // "<kms-key-resource>|<base64-ciphertext>"
}

func (r *EmbyrSecretResolver) Resolve(ctx context.Context) (string, error) {
	// Parse "<keyResource>|<ciphertext>"
	sep := -1
	for i, c := range r.encryptedDSN {
		if c == '|' {
			sep = i
			break
		}
	}
	if sep < 0 {
		return "", fmt.Errorf("tenancy: embyr_secret: malformed credential_ref (missing '|')")
	}
	keyResource := r.encryptedDSN[:sep]
	ciphertext := []byte(r.encryptedDSN[sep+1:])

	client, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		return "", fmt.Errorf("tenancy: kms client: %w", err)
	}
	defer client.Close()

	resp, err := client.Decrypt(ctx, &kmspb.DecryptRequest{
		Name:       keyResource,
		Ciphertext: ciphertext,
	})
	if err != nil {
		return "", fmt.Errorf("tenancy: kms decrypt: %w", err)
	}
	return string(resp.Plaintext), nil
}
```

- [ ] **Step 5: Write agent.go (stub)**

```go
// internal/tenancy/agent.go
package tenancy

import (
	"context"
	"fmt"
)

// AgentResolver resolves a DSN through the embyr-agent tunnel protocol.
// Implemented in Plan 3. This stub returns an error so the type compiles
// and the factory can reference it without the tunnel server being present.
type AgentResolver struct {
	agentID string
}

func (r *AgentResolver) Resolve(_ context.Context) (string, error) {
	return "", fmt.Errorf("tenancy: agent resolver not yet implemented (agentID=%s)", r.agentID)
}
```

- [ ] **Step 6: Compile check**

```bash
go build ./internal/tenancy/...
```

Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add internal/tenancy/
git commit -m "feat(tenancy): CredentialResolver interface and GCP/AWS/Embyr/Agent implementations"
```

---

## Task 7: internal/tenancy — AdapterFactory

**Files:**
- Create: `internal/tenancy/factory.go`
- Create: `internal/tenancy/factory_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/tenancy/factory_test.go
package tenancy_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/petereon/embyr/internal/registry"
	"github.com/petereon/embyr/internal/tenancy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRegistry is an in-memory Client for tests.
type fakeRegistry struct {
	tenants map[string]*registry.Tenant
}

func (f *fakeRegistry) Get(_ context.Context, projectID, databaseID string) (*registry.Tenant, error) {
	t, ok := f.tenants[projectID+"/"+databaseID]
	if !ok {
		return nil, registry.ErrTenantNotFound
	}
	return t, nil
}
func (f *fakeRegistry) Close() {}

func TestAdapterFactory_UnknownTenant(t *testing.T) {
	reg := &fakeRegistry{tenants: map[string]*registry.Tenant{}}
	factory := tenancy.NewAdapterFactory(reg, 10)
	defer factory.Close()

	_, _, err := factory.Get(context.Background(), "nope", "nope")
	assert.ErrorIs(t, err, registry.ErrTenantNotFound)
}

func TestAdapterFactory_SuspendedTenant(t *testing.T) {
	reg := &fakeRegistry{tenants: map[string]*registry.Tenant{
		"acme/prod": {
			ID: "t1", ProjectID: "acme", DatabaseID: "prod",
			Status: registry.TenantStatusSuspended,
		},
	}}
	factory := tenancy.NewAdapterFactory(reg, 10)
	defer factory.Close()

	_, _, err := factory.Get(context.Background(), "acme", "prod")
	assert.ErrorIs(t, err, tenancy.ErrTenantSuspended)
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./internal/tenancy/... 2>&1 | head -15
```

Expected: compile error — `tenancy.NewAdapterFactory`, `tenancy.ErrTenantSuspended` undefined.

- [ ] **Step 3: Write factory.go**

```go
// internal/tenancy/factory.go
package tenancy

import (
	"context"
	"errors"
	"fmt"
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
	cancel     context.CancelFunc
}

// AdapterFactory lazily creates and caches StorageAdapters per tenant.
// Evicted entries have their connection pools closed gracefully.
type AdapterFactory struct {
	reg  registry.Client
	log  *zap.Logger
	mu   sync.Mutex
	cache *lru.Cache[string, *entry]
}

// NewAdapterFactory returns a factory with an LRU cap of capacity entries.
func NewAdapterFactory(reg registry.Client, capacity int) *AdapterFactory {
	cache, _ := lru.NewWithEvict[string, *entry](capacity, func(_ string, e *entry) {
		if e.cancel != nil {
			e.cancel()
		}
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

func cacheKey(projectID, databaseID string) string {
	return projectID + "/" + databaseID
}

// Get returns (adapter, authConfig) for (projectID, databaseID).
// On cache miss: resolves credential, opens Postgres pool, pings, caches.
// Returns ErrTenantNotFound or ErrTenantSuspended without opening a connection.
func (f *AdapterFactory) Get(ctx context.Context, projectID, databaseID string) (store.StorageAdapter, *auth.Config, error) {
	key := cacheKey(projectID, databaseID)

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

	// Append search_path so all queries land in the tenant's schema.
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
func (f *AdapterFactory) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cache.Purge()
}

// appendSearchPath adds search_path=<schema> to a Postgres DSN.
func appendSearchPath(dsn, schema string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		// Not a URL-format DSN — try keyword format: "host=... search_path=..."
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
	// Wrap in a minimal Config so auth.New can process it.
	cfg := &config.Config{Auth: authCfg}
	return auth.New(cfg, zap.NewNop())
}
```

- [ ] **Step 4: Add unmarshalAuthConfig helper (same file)**

Append to `factory.go`:

```go
// unmarshalAuthConfig populates fields from raw JSON using json field names
// matching config.AuthConfig's mapstructure tags.
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
```

Add `"encoding/json"` to the import block in `factory.go`.

- [ ] **Step 5: Run tests to verify they pass**

```bash
go test ./internal/tenancy/... -v -run TestAdapterFactory
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/tenancy/factory.go internal/tenancy/factory_test.go
git commit -m "feat(tenancy): AdapterFactory with LRU cache and credential resolution"
```

---

## Task 8: internal/tenancy — middleware

**Files:**
- Create: `internal/tenancy/middleware.go`
- Create: `internal/tenancy/middleware_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/tenancy/middleware_test.go
package tenancy_test

import (
	"context"
	"testing"

	"github.com/petereon/embyr/internal/tenancy"
	"github.com/stretchr/testify/assert"
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
	}
	for _, tc := range cases {
		p, d, ok := tenancy.ParseTenantKey(tc.path)
		assert.Equal(t, tc.ok, ok, "path=%q", tc.path)
		assert.Equal(t, tc.project, p, "path=%q", tc.path)
		assert.Equal(t, tc.database, d, "path=%q", tc.path)
	}
}
```

- [ ] **Step 2: Run to verify failure**

```bash
go test ./internal/tenancy/... -run TestParseTenantKey 2>&1 | head -10
```

Expected: compile error — `tenancy.ParseTenantKey` undefined.

- [ ] **Step 3: Write middleware.go**

```go
// internal/tenancy/middleware.go
package tenancy

import (
	"context"
	"net/http"
	"strings"

	"github.com/petereon/embyr/internal/auth"
	"github.com/petereon/embyr/internal/registry"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ParseTenantKey extracts (projectID, databaseID) from a Firestore resource path.
// Accepts full paths like "projects/p/databases/d/documents/..." or
// partial paths like "projects/p/databases/d".
func ParseTenantKey(path string) (projectID, databaseID string, ok bool) {
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

type factoryIface interface {
	Get(ctx context.Context, projectID, databaseID string) (interface {
		// StorageAdapter is opaque here; factory returns it as the first value.
	}, *auth.Config, error)
}

// UnaryInterceptor returns a gRPC unary interceptor that resolves the tenant
// adapter from the request path and injects it + validates auth before dispatch.
func UnaryInterceptor(factory *AdapterFactory, log *zap.Logger) grpc.UnaryServerInterceptor {
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
func StreamInterceptor(factory *AdapterFactory, log *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrapped := &peekStream{ServerStream: ss, factory: factory, log: log}
		return handler(srv, wrapped)
	}
}

// peekStream wraps a grpc.ServerStream. On the first RecvMsg call it extracts
// the tenant key from the decoded message and injects the adapter into context.
type peekStream struct {
	grpc.ServerStream
	factory  *AdapterFactory
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

type contextStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextStream) Context() context.Context { return s.ctx }

// HTTPMiddleware wraps an http.Handler, extracting the tenant key from the
// URL path and injecting the adapter into the request context.
func HTTPMiddleware(factory *AdapterFactory, log *zap.Logger, next http.Handler) http.Handler {
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

// resolveTenant looks up the tenant from the path, checks status, and
// injects the StorageAdapter + AuthInfo into the context.
// Both "tenant not found" and "tenant suspended" return Unauthenticated
// with a generic message to prevent tenant enumeration.
func resolveTenant(ctx context.Context, factory *AdapterFactory, log *zap.Logger, path string) (context.Context, error) {
	projectID, databaseID, ok := ParseTenantKey(path)
	if !ok {
		// No tenant key in path — pass through (single-tenant or health endpoints).
		return ctx, nil
	}

	adapter, authCfg, err := factory.Get(ctx, projectID, databaseID)
	if err != nil {
		switch {
		case isErrSuspended(err) || isErrNotFound(err):
			return ctx, status.Error(codes.Unauthenticated, "unauthenticated")
		default:
			log.Warn("tenancy: factory error", zap.Error(err),
				zap.String("project", projectID), zap.String("database", databaseID))
			return ctx, status.Error(codes.Internal, "internal error")
		}
	}

	ctx = WithAdapter(ctx, adapter)
	ctx = WithAuthInfo(ctx, AuthInfo{
		Mode:   authCfg.Mode(),
		Config: authCfg.RawConfig(),
	})
	return ctx, nil
}

func isErrNotFound(err error) bool {
	return err != nil && (err == registry.ErrTenantNotFound ||
		strings.Contains(err.Error(), "not found"))
}

func isErrSuspended(err error) bool {
	return err == ErrTenantSuspended
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
```

- [ ] **Step 4: Add `Mode()` and `RawConfig()` to auth.Config**

Open `internal/auth/interceptor.go` and add two accessor methods after the `Config` struct definition (around line 47):

```go
// Mode returns the auth mode string for this config.
func (c *Config) Mode() string { return c.mode }

// RawConfig returns the raw auth config bytes.
func (c *Config) RawConfig() []byte { return c.rawConfig }
```

Add fields to the `Config` struct:

```go
type Config struct {
	Unary     grpc.UnaryServerInterceptor
	Stream    grpc.StreamServerInterceptor
	Creds     credentials.TransportCredentials
	mode      string
	rawConfig []byte
}
```

Update every return in `auth.New` to include `mode` and `rawConfig`:

In `auth.New`, wrap returns to attach mode — e.g. for `"none"`:
```go
return &Config{Unary: noneUnary, Stream: noneStream, mode: "none"}, nil
```

For `"key"`:
```go
return &Config{Unary: keyUnary(cfg.Auth.Key), Stream: keyStream(cfg.Auth.Key), mode: "key"}, nil
```

For `"google"`:
```go
return &Config{Unary: googleUnary(cfg.Auth.GoogleProjectID), Stream: googleStream(cfg.Auth.GoogleProjectID), mode: "google"}, nil
```

For `"mtls"`:
```go
return &Config{Unary: mtlsUnary, Stream: mtlsStream, Creds: creds, mode: "mtls"}, nil
```

- [ ] **Step 5: Run tests**

```bash
go test ./internal/tenancy/... -v -run TestParseTenantKey
go test ./internal/auth/... -v
```

Expected: both pass.

- [ ] **Step 6: Commit**

```bash
git add internal/tenancy/middleware.go internal/tenancy/middleware_test.go internal/auth/interceptor.go
git commit -m "feat(tenancy): gRPC+HTTP middleware for tenant path extraction and adapter injection"
```

---

## Task 9: internal/auth — ValidateForTenant

**Files:**
- Modify: `internal/auth/interceptor.go`
- Modify: `internal/auth/interceptor_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/auth/interceptor_test.go`:

```go
func TestValidateForTenant_None(t *testing.T) {
	ctx := context.Background()
	// mode=none should always pass, no token needed
	err := auth.ValidateForTenant(ctx, "none", nil)
	require.NoError(t, err)
}

func TestValidateForTenant_Key_Valid(t *testing.T) {
	cfg := []byte(`{"key":"secret123"}`)
	md := metadata.New(map[string]string{"authorization": "Bearer secret123"})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	err := auth.ValidateForTenant(ctx, "key", cfg)
	require.NoError(t, err)
}

func TestValidateForTenant_Key_Invalid(t *testing.T) {
	cfg := []byte(`{"key":"secret123"}`)
	md := metadata.New(map[string]string{"authorization": "Bearer wrong"})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	err := auth.ValidateForTenant(ctx, "key", cfg)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}
```

- [ ] **Step 2: Run to verify failure**

```bash
go test ./internal/auth/... -run TestValidateForTenant 2>&1 | head -10
```

Expected: compile error — `auth.ValidateForTenant` undefined.

- [ ] **Step 3: Implement ValidateForTenant**

Add to `internal/auth/interceptor.go`:

```go
// ValidateForTenant validates the incoming request token against a tenant's
// stored auth mode and config. Used by the tenancy middleware.
// rawConfig is a JSON object whose fields match config.AuthConfig json tags.
func ValidateForTenant(ctx context.Context, mode string, rawConfig []byte) error {
	switch mode {
	case "none", "":
		return nil
	case "key":
		var cfg struct {
			Key string `json:"key"`
		}
		if err := json.Unmarshal(rawConfig, &cfg); err != nil {
			return status.Errorf(codes.Internal, "auth: parse key config: %v", err)
		}
		return validateKey(ctx, cfg.Key)
	case "google":
		var cfg struct {
			GoogleProjectID string `json:"google_project_id"`
		}
		if err := json.Unmarshal(rawConfig, &cfg); err != nil {
			return status.Errorf(codes.Internal, "auth: parse google config: %v", err)
		}
		return validateGoogleToken(ctx, cfg.GoogleProjectID)
	case "mtls":
		return validateMTLS(ctx)
	default:
		return status.Errorf(codes.Unauthenticated, "auth: unknown mode %q", mode)
	}
}
```

Add `"encoding/json"` to imports in `interceptor.go`.

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./internal/auth/... -v -run TestValidateForTenant
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add internal/auth/interceptor.go internal/auth/interceptor_test.go
git commit -m "feat(auth): ValidateForTenant for per-request tenant auth validation"
```

---

## Task 10: internal/server — adapter() method and NewMultiTenant

**Files:**
- Modify: `internal/server/server.go`

- [ ] **Step 1: Read the firestoreServer struct (lines 25-32 of handlers.go)**

The struct is defined in `internal/server/handlers.go:26`:

```go
type firestoreServer struct {
    db       store.StorageAdapter
    log      *zap.Logger
    registry *listen.Registry
}
```

- [ ] **Step 2: Add adapter() method to handlers.go**

After the struct definition (after line ~32 in `handlers.go`), add:

```go
// adapter returns the StorageAdapter for this request.
// In multi-tenant mode the middleware injects a per-tenant adapter via context.
// In single-tenant mode it falls back to s.db.
func (s *firestoreServer) adapter(ctx context.Context) store.StorageAdapter {
	if a := tenancy.AdapterFromCtx(ctx); a != nil {
		return a
	}
	return s.db
}
```

Add import `"github.com/petereon/embyr/internal/tenancy"` to `handlers.go`.

- [ ] **Step 3: Add NewMultiTenant to server.go**

In `internal/server/server.go`, add after the `New` function:

```go
// NewMultiTenant creates a Server in multi-tenant mode. The factory resolves
// a StorageAdapter per (projectId, databaseId) from each incoming request path.
// db is nil in this mode; adapter() reads from context instead.
func NewMultiTenant(cfg *config.Config, factory *tenancy.AdapterFactory, log *zap.Logger) (*Server, error) {
	// Build a placeholder auth config (interceptors are per-tenant in multi-tenant mode).
	// We still need gRPC server options; use none-mode auth as the outer shell —
	// per-tenant auth is enforced by the tenancy middleware, not the gRPC interceptors.
	authCfg := &auth.Config{Unary: tenancy.UnaryInterceptor(factory, log), Stream: tenancy.StreamInterceptor(factory, log)}

	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(loggingUnaryInterceptor(log), authCfg.Unary),
		grpc.ChainStreamInterceptor(loggingStreamInterceptor(log), authCfg.Stream),
	}

	grpcSrv := grpc.NewServer(opts...)
	reg := listen.NewRegistry()
	fs := &firestoreServer{db: nil, log: log, registry: reg}
	firestorev1.RegisterFirestoreServer(grpcSrv, fs)
	reflection.Register(grpcSrv)

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

	h := health.New(nil) // no single adapter in multi-tenant mode
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.Healthz)
	mux.HandleFunc("/readyz", h.Readyz)

	wcMgr := webchannel.NewManager()
	wcHandler := webchannel.NewHandler(wcMgr, fs.Listen)
	mux.Handle("/google.firestore.v1.Firestore/Listen/channel", wcHandler)
	wcWriteHandler := webchannel.NewWriteHandler(wcMgr, fs.Write)
	mux.Handle("/google.firestore.v1.Firestore/Write/channel", wcWriteHandler)

	// Wrap the full mux in tenancy HTTP middleware so REST requests also get
	// the adapter injected (webchannel paths are streaming — handled by gRPC interceptors).
	tenancyMux := tenancy.HTTPMiddleware(factory, log, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":runQuery") {
			serveRunQuery(w, r, fs)
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":batchGet") {
			serveBatchGetDocuments(w, r, fs)
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":runAggregationQuery") {
			serveRunAggregationQuery(w, r, fs)
			return
		}
		gwMux.ServeHTTP(w, r)
	}))
	mux.Handle("/", tenancyMux)

	return &Server{
		cfg:        cfg,
		db:         nil,
		log:        log,
		grpcServer: grpcSrv,
		restMux:    mux,
		grpcWebSrv: grpcWebSrv,
		fs:         fs,
		wcMgr:      wcMgr,
	}, nil
}
```

Add `"github.com/petereon/embyr/internal/tenancy"` to `server.go` imports.

- [ ] **Step 4: Update health.New to accept nil**

Open `internal/health/health.go`. If `Ping` panics on nil adapter, guard it:

```go
func (h *Health) Readyz(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	// existing logic
}
```

- [ ] **Step 5: Compile check**

```bash
go build ./...
```

Expected: no errors.

- [ ] **Step 6: Verify existing tests still pass**

```bash
go test ./internal/server/... ./internal/auth/... ./internal/store/... -count=1
```

Expected: all pass (single-tenant path unchanged).

- [ ] **Step 7: Commit**

```bash
git add internal/server/server.go internal/server/handlers.go internal/health/health.go
git commit -m "feat(server): NewMultiTenant constructor and per-request adapter() helper"
```

---

## Task 11: Replace s.db with s.adapter(ctx) in handlers

**Files:**
- Modify: `internal/server/handlers.go`
- Modify: `internal/server/aggregation.go`
- Modify: `internal/server/transactions.go`

- [ ] **Step 1: Replace in handlers.go**

Search for all occurrences of `s.db.` in `handlers.go` and replace with `s.adapter(ctx).`:

```bash
# Verify what needs changing
grep -n "s\.db\." internal/server/handlers.go
```

For each hit, replace `s.db.` with `s.adapter(ctx).`. The `ctx` variable is always in scope (every handler has `ctx context.Context` as first param or `stream.Context()`).

For `applyWriteBatch` (which receives `ctx` as parameter):
```go
// Before:
d, err := s.db.GetDocument(ctx, ...)
// After:
d, err := s.adapter(ctx).GetDocument(ctx, ...)
```

For `Write` stream handler, `ctx := stream.Context()` is at the top — use that same `ctx`.

- [ ] **Step 2: Replace in aggregation.go**

```bash
grep -n "s\.db\." internal/server/aggregation.go
```

Replace all `s.db.` → `s.adapter(ctx).` — `ctx` is the first parameter in every method.

- [ ] **Step 3: Replace in transactions.go**

```bash
grep -n "s\.db\." internal/server/transactions.go
```

Replace all `s.db.` → `s.adapter(ctx).`.

Note: `BatchWrite` uses `s.db.WithTransaction` and `s.applyWriteBatch`. Both have access to `ctx`. Replace accordingly.

- [ ] **Step 4: Verify no s.db. calls remain in handler files**

```bash
grep -n "s\.db\." internal/server/handlers.go internal/server/aggregation.go internal/server/transactions.go
```

Expected: no output.

- [ ] **Step 5: Run all existing tests**

```bash
go test ./... -count=1 2>&1 | tail -30
```

Expected: all pass. The single-tenant tests work because `s.adapter(ctx)` falls back to `s.db` when no adapter is in context.

- [ ] **Step 6: Commit**

```bash
git add internal/server/handlers.go internal/server/aggregation.go internal/server/transactions.go
git commit -m "feat(server): route all handler DB calls through adapter(ctx) for multi-tenant support"
```

---

## Task 12: cmd/data — multi-tenant entry point

**Files:**
- Create: `cmd/data/main.go`

- [ ] **Step 1: Write cmd/data/main.go**

```go
// cmd/data/main.go — multi-tenant data plane entry point for Cloud Run.
package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/petereon/embyr/internal/config"
	"github.com/petereon/embyr/internal/registry"
	"github.com/petereon/embyr/internal/server"
	"github.com/petereon/embyr/internal/tenancy"
	"go.uber.org/zap"
)

func main() {
	configPath := flag.String("config", "", "path to config YAML file (optional)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("data: load config: %v", err)
	}

	var zapCfg zap.Config
	if cfg.Log.Format == "json" {
		zapCfg = zap.NewProductionConfig()
	} else {
		zapCfg = zap.NewDevelopmentConfig()
	}
	logger, err := zapCfg.Build()
	if err != nil {
		log.Fatalf("data: build logger: %v", err)
	}
	defer logger.Sync() //nolint:errcheck

	registryDSN := os.Getenv("EMBYR_REGISTRY_DSN")
	if registryDSN == "" {
		logger.Fatal("data: EMBYR_REGISTRY_DSN env var required")
	}

	regDB, err := sql.Open("pgx", registryDSN)
	if err != nil {
		logger.Fatal("data: open registry DB", zap.Error(err))
	}
	defer regDB.Close()

	regClient := registry.NewPostgresClient(regDB, 60*time.Second)
	defer regClient.Close()

	factory := tenancy.NewAdapterFactory(regClient, 200).WithLogger(logger)
	defer factory.Close()

	srv, err := server.NewMultiTenant(cfg, factory, logger)
	if err != nil {
		logger.Fatal("data: server init", zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("embyr data plane starting",
		zap.Int("grpc_port", cfg.Server.GRPCPort),
		zap.Int("rest_port", cfg.Server.RESTPort),
	)

	if err := srv.Run(ctx); err != nil {
		logger.Fatal("data: server exited", zap.Error(err))
	}
}
```

- [ ] **Step 2: Build check**

```bash
go build ./cmd/data/...
```

Expected: produces `embyr-data` binary (or no errors when using `go build`).

- [ ] **Step 3: Smoke test — start with no EMBYR_REGISTRY_DSN set**

```bash
go run ./cmd/data/ 2>&1 | head -5
```

Expected: log line `"data: EMBYR_REGISTRY_DSN env var required"` then exit.

- [ ] **Step 4: Run full test suite one final time**

```bash
go test ./... -count=1 2>&1 | tail -20
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add cmd/data/
git commit -m "feat(cmd/data): multi-tenant Cloud Run entry point"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task covering it |
|---|---|
| `internal/registry` — Tenant type + PostgresClient | Tasks 2, 3 |
| `internal/migrations` — schema-aware Runner | Task 4 |
| `internal/tenancy` — CredentialResolver (GCP/AWS/Embyr/Agent) | Task 6 |
| `internal/tenancy` — AdapterFactory with LRU | Task 7 |
| `internal/tenancy` — gRPC + HTTP middleware | Task 8 |
| `auth.ValidateForTenant` for per-tenant auth | Task 9 |
| `server.NewMultiTenant` + `adapter()` fallback | Task 10 |
| Replace `s.db` with `s.adapter(ctx)` | Task 11 |
| `cmd/data` entry point | Task 12 |
| `search_path` isolation per tenant | Task 7 (`appendSearchPath`) |
| Single-tenant / existing tests unchanged | Task 10 step 6, Task 11 step 5 |
| `cmd/embyr` untouched | Not in file map — zero edits confirmed |

**No placeholders found.** All steps have exact code or exact commands.

**Type consistency check:**
- `registry.Client` interface defined in Task 3, implemented by `*PostgresClient`, used by `AdapterFactory` in Task 7 ✓
- `tenancy.AdapterFromCtx` defined in Task 5, used in Task 10 (`adapter()` method) ✓
- `auth.Config.Mode()` / `auth.Config.RawConfig()` added in Task 8, used in `resolveTenant` ✓
- `tenancy.UnaryInterceptor` / `StreamInterceptor` defined in Task 8, used in Task 10 ✓
- `migrations.NewRunner` defined in Task 4 — used by control plane in Plan 2 (not Plan 1) ✓

---

Plan complete and saved to `docs/superpowers/plans/2026-05-02-core-tenancy-layer.md`.

**Two execution options:**

**1. Subagent-Driven (recommended)** — fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
