# embyr — Plan 1: Foundation & Protocol Scaffold

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a compilable Go binary that connects to SQLite or PostgreSQL, runs schema migrations, serves the Firestore gRPC + REST API surface (all RPCs return `UNIMPLEMENTED`), and responds to `/healthz` and `/readyz`.

**Architecture:** gRPC-first with grpc-gateway REST transcoding. `buf` pulls Firestore v1 proto files from the Buf Schema Registry and drives codegen for both protocols. A `StorageAdapter` interface decouples all server logic from the database backend. This plan is scaffold only — no document business logic is implemented.

**Tech Stack:** Go 1.22, buf v2 (proto codegen), grpc-gateway v2, modernc.org/sqlite (pure-Go, no CGo), pgx/v5 (PostgreSQL), golang-migrate/migrate/v4 (schema migrations), viper (config YAML + env vars), zap (structured logging), testify (assertions)

> **Note on module path:** Steps below use `github.com/embyr/embyr`. Replace this with your actual GitHub path before running `go mod init`.

> **This is Plan 1 of 5.** Plans 2–5 build on this foundation:
> - Plan 2: Document CRUD + Auth middleware
> - Plan 3: Query planner + Transactions
> - Plan 4: Real-time listeners
> - Plan 5: Deployment + Observability

---

## File Map

```
embyr/
├── cmd/embyr/main.go                        # Binary entry point
├── internal/
│   ├── config/
│   │   ├── config.go                          # Config struct + Load()
│   │   └── config_test.go
│   ├── store/
│   │   ├── adapter.go                         # StorageAdapter interface + shared types
│   │   ├── sqlite/
│   │   │   ├── sqlite.go                      # SQLite adapter (Ping, Close, Migrate)
│   │   │   └── sqlite_test.go
│   │   └── postgres/
│   │       ├── postgres.go                    # PostgreSQL adapter (Ping, Close, Migrate)
│   │       └── postgres_test.go
│   ├── server/
│   │   ├── server.go                          # gRPC + grpc-gateway setup
│   │   └── server_test.go
│   └── health/
│       ├── health.go                          # /healthz and /readyz handlers
│       └── health_test.go
├── migrations/
│   ├── sqlite/
│   │   ├── 000001_init.up.sql
│   │   └── 000001_init.down.sql
│   └── postgres/
│       ├── 000001_init.up.sql
│       └── 000001_init.down.sql
├── gen/go/                                    # Generated proto code (committed to git)
├── buf.yaml                                   # Buf module config
├── buf.gen.yaml                               # Buf codegen config
├── Makefile                                   # proto, build, test targets
└── go.mod
```

---

## Task 1: Initialize Go Module and Project Scaffold

**Files:**
- Create: `go.mod`
- Create: `Makefile`
- Modify: `.gitignore`

- [ ] **Step 1: Initialize the Go module**

```bash
cd /path/to/embyr
go mod init github.com/embyr/embyr
```

Expected output: `go: creating new go.mod: module github.com/embyr/embyr`

- [ ] **Step 2: Create the directory structure**

```bash
mkdir -p cmd/embyr
mkdir -p internal/config
mkdir -p internal/store/sqlite
mkdir -p internal/store/postgres
mkdir -p internal/server
mkdir -p internal/health
mkdir -p migrations/sqlite
mkdir -p migrations/postgres
mkdir -p gen/go
```

- [ ] **Step 3: Add Go entries to .gitignore**

Append to the existing `.gitignore`:

```
# Go binaries
embyr
/dist/

# Go test cache
*.test

# Go build artifacts
*.out
```

- [ ] **Step 4: Write the Makefile**

```makefile
.PHONY: proto build test lint clean

BINARY=embyr
MODULE=github.com/embyr/embyr

proto:
	buf generate

build:
	CGO_ENABLED=0 go build -o $(BINARY) ./cmd/embyr

test:
	go test ./... -v -count=1

lint:
	golangci-lint run ./...

clean:
	rm -f $(BINARY)
	rm -rf gen/go
```

- [ ] **Step 5: Install buf CLI**

```bash
go install github.com/bufbuild/buf/cmd/buf@latest
```

Expected: `buf --version` prints a version string.

- [ ] **Step 6: Install Go codegen plugins**

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
```

- [ ] **Step 7: Commit**

```bash
git add go.mod Makefile .gitignore cmd/ internal/ migrations/ gen/
git commit -m "chore: initialize Go module and project structure"
```

---

## Task 2: Configure buf and Generate Firestore Proto Code

**Files:**
- Create: `buf.yaml`
- Create: `buf.gen.yaml`
- Create: `gen/go/google/firestore/v1/*.go` (generated)

- [ ] **Step 1: Write buf.yaml**

```yaml
version: v2
deps:
  - buf.build/googleapis/googleapis
  - buf.build/grpc-ecosystem/grpc-gateway
```

- [ ] **Step 2: Write buf.gen.yaml**

```yaml
version: v2
inputs:
  - module: buf.build/googleapis/googleapis
    paths:
      - google/firestore/v1
plugins:
  - remote: buf.build/protocolbuffers/go
    out: gen/go
    opt:
      - paths=source_relative
  - remote: buf.build/grpc/go
    out: gen/go
    opt:
      - paths=source_relative
  - remote: buf.build/grpc-ecosystem/gateway
    out: gen/go
    opt:
      - paths=source_relative
      - generate_unbound_methods=true
```

- [ ] **Step 3: Update buf dependencies**

```bash
buf dep update
```

Expected: creates `buf.lock` pinning exact BSR commit SHAs.

- [ ] **Step 4: Generate proto code**

```bash
buf generate
```

Expected: `gen/go/google/firestore/v1/` is populated with `*.pb.go`, `*_grpc.pb.go`, and `*.pb.gw.go` files.

- [ ] **Step 5: Add generated code dependencies to go.mod**

```bash
go get google.golang.org/grpc
go get google.golang.org/protobuf
go get github.com/grpc-ecosystem/grpc-gateway/v2
go mod tidy
```

- [ ] **Step 6: Verify generated code compiles**

```bash
go build ./gen/...
```

Expected: exits 0 with no output.

- [ ] **Step 7: Commit**

```bash
git add buf.yaml buf.gen.yaml buf.lock gen/ go.mod go.sum
git commit -m "chore: add buf config and commit generated Firestore proto stubs"
```

---

## Task 3: Config Struct and YAML Loading

**Files:**
- Create: `internal/config/config.go`
- Create: `internal/config/config_test.go`

- [ ] **Step 1: Install viper**

```bash
go get github.com/spf13/viper
go mod tidy
```

- [ ] **Step 2: Write the failing test**

```go
// internal/config/config_test.go
package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/embyr/embyr/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_Defaults(t *testing.T) {
	cfg, err := config.Load("")
	require.NoError(t, err)
	assert.Equal(t, 8080, cfg.Server.GRPCPort)
	assert.Equal(t, 8081, cfg.Server.RESTPort)
	assert.Equal(t, "none", cfg.Auth.Mode)
	assert.Equal(t, "sqlite", cfg.Backend.Type)
	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, "json", cfg.Log.Format)
	assert.Equal(t, "60s", cfg.Transactions.TTL)
	assert.Equal(t, "30s", cfg.Transactions.SweepInterval)
}

func TestLoad_FromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
server:
  grpc_port: 9090
  rest_port: 9091
auth:
  mode: key
  key: "secret"
backend:
  type: postgres
  postgres:
    dsn: "postgres://user:pass@localhost/db"
    max_conns: 10
`
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0600))

	cfg, err := config.Load(path)
	require.NoError(t, err)
	assert.Equal(t, 9090, cfg.Server.GRPCPort)
	assert.Equal(t, 9091, cfg.Server.RESTPort)
	assert.Equal(t, "key", cfg.Auth.Mode)
	assert.Equal(t, "secret", cfg.Auth.Key)
	assert.Equal(t, "postgres", cfg.Backend.Type)
	assert.Equal(t, "postgres://user:pass@localhost/db", cfg.Backend.Postgres.DSN)
	assert.Equal(t, 10, cfg.Backend.Postgres.MaxConns)
}

func TestLoad_EnvOverride(t *testing.T) {
	t.Setenv("EMBYR_AUTH_KEY", "env-override-key")
	t.Setenv("EMBYR_BACKEND_TYPE", "sqlite")

	cfg, err := config.Load("")
	require.NoError(t, err)
	assert.Equal(t, "env-override-key", cfg.Auth.Key)
	assert.Equal(t, "sqlite", cfg.Backend.Type)
}
```

- [ ] **Step 3: Run test to confirm it fails**

```bash
go test ./internal/config/... -v -run TestLoad
```

Expected: compile error — `config` package does not exist yet.

- [ ] **Step 4: Install testify**

```bash
go get github.com/stretchr/testify
go mod tidy
```

- [ ] **Step 5: Implement config.go**

```go
// internal/config/config.go
package config

import (
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Server       ServerConfig      `mapstructure:"server"`
	Auth         AuthConfig        `mapstructure:"auth"`
	Backend      BackendConfig     `mapstructure:"backend"`
	Transactions TransactionConfig `mapstructure:"transactions"`
	Log          LogConfig         `mapstructure:"log"`
}

type ServerConfig struct {
	GRPCPort int       `mapstructure:"grpc_port"`
	RESTPort int       `mapstructure:"rest_port"`
	TLS      TLSConfig `mapstructure:"tls"`
}

type TLSConfig struct {
	Cert string `mapstructure:"cert"`
	Key  string `mapstructure:"key"`
}

type AuthConfig struct {
	Mode            string `mapstructure:"mode"`
	Key             string `mapstructure:"key"`
	GoogleProjectID string `mapstructure:"google_project_id"`
	MTLSCACert      string `mapstructure:"mtls_ca"`
}

type BackendConfig struct {
	Type     string         `mapstructure:"type"`
	Postgres PostgresConfig `mapstructure:"postgres"`
	SQLite   SQLiteConfig   `mapstructure:"sqlite"`
}

type PostgresConfig struct {
	DSN      string `mapstructure:"dsn"`
	MaxConns int    `mapstructure:"max_conns"`
}

type SQLiteConfig struct {
	Path string `mapstructure:"path"`
}

type TransactionConfig struct {
	TTL           string `mapstructure:"ttl"`
	SweepInterval string `mapstructure:"sweep_interval"`
}

type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// Load reads a YAML config file (path may be empty for defaults only).
// Environment variables prefixed with EMBYR_ override any config file value.
// Key mapping: EMBYR_AUTH_KEY → auth.key, EMBYR_BACKEND_TYPE → backend.type
func Load(path string) (*Config, error) {
	v := viper.New()

	// Defaults
	v.SetDefault("server.grpc_port", 8080)
	v.SetDefault("server.rest_port", 8081)
	v.SetDefault("auth.mode", "none")
	v.SetDefault("backend.type", "sqlite")
	v.SetDefault("backend.sqlite.path", "embyr.db")
	v.SetDefault("backend.postgres.max_conns", 25)
	v.SetDefault("transactions.ttl", "60s")
	v.SetDefault("transactions.sweep_interval", "30s")
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")

	// Env vars
	v.SetEnvPrefix("EMBYR")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// File
	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, err
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
```

- [ ] **Step 6: Run tests to confirm they pass**

```bash
go test ./internal/config/... -v -run TestLoad
```

Expected:
```
--- PASS: TestLoad_Defaults (0.00s)
--- PASS: TestLoad_FromFile (0.00s)
--- PASS: TestLoad_EnvOverride (0.00s)
PASS
```

- [ ] **Step 7: Commit**

```bash
git add internal/config/ go.mod go.sum
git commit -m "feat: add config parsing with YAML and env var support"
```

---

## Task 4: Storage Adapter Interface

**Files:**
- Create: `internal/store/adapter.go`

No tests in this task — the interface is verified by the adapter implementations in Tasks 6 and 7.

- [ ] **Step 1: Write adapter.go**

```go
// internal/store/adapter.go
package store

import "context"

// StorageAdapter is the single interface both the SQLite and PostgreSQL backends implement.
// Methods are added incrementally across plans:
//   - Plan 1: Ping, Close, Migrate
//   - Plan 2: document CRUD (GetDocument, SetDocument, UpdateDocument, DeleteDocument, ListDocuments)
//   - Plan 3: RunQuery, BeginTransaction, CommitTransaction, RollbackTransaction, BatchWrite
//   - Plan 4: WatchCollection, UnwatchCollection
type StorageAdapter interface {
	// Ping checks that the database is reachable. Used by /readyz.
	Ping(ctx context.Context) error

	// Migrate runs all pending schema migrations.
	Migrate(ctx context.Context) error

	// Close releases all database connections.
	Close() error
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/store/...
```

Expected: exits 0.

- [ ] **Step 3: Commit**

```bash
git add internal/store/adapter.go
git commit -m "feat: define StorageAdapter interface"
```

---

## Task 5: SQL Migration Files

**Files:**
- Create: `migrations/sqlite/000001_init.up.sql`
- Create: `migrations/sqlite/000001_init.down.sql`
- Create: `migrations/postgres/000001_init.up.sql`
- Create: `migrations/postgres/000001_init.down.sql`

- [ ] **Step 1: Write SQLite up migration**

```sql
-- migrations/sqlite/000001_init.up.sql

CREATE TABLE IF NOT EXISTS documents (
    path        TEXT    PRIMARY KEY,
    collection  TEXT    NOT NULL,
    parent      TEXT,
    data        TEXT,   -- JSON stored as text; query via JSON1 extension
    created_at  TEXT    NOT NULL,
    updated_at  TEXT    NOT NULL,
    version     INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_documents_collection ON documents(collection);
CREATE INDEX IF NOT EXISTS idx_documents_parent     ON documents(parent);
CREATE INDEX IF NOT EXISTS idx_documents_updated_at ON documents(updated_at);

CREATE TABLE IF NOT EXISTS indexes (
    id          TEXT    PRIMARY KEY,
    collection  TEXT    NOT NULL,
    fields      TEXT    NOT NULL, -- JSON array [{field, order}]
    state       TEXT    NOT NULL DEFAULT 'READY'
);

CREATE TABLE IF NOT EXISTS transactions (
    id          TEXT    PRIMARY KEY,
    started_at  TEXT    NOT NULL,
    reads       TEXT,             -- JSON array of paths read
    expires_at  TEXT    NOT NULL
);
```

- [ ] **Step 2: Write SQLite down migration**

```sql
-- migrations/sqlite/000001_init.down.sql

DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS indexes;
DROP INDEX IF EXISTS idx_documents_updated_at;
DROP INDEX IF EXISTS idx_documents_parent;
DROP INDEX IF EXISTS idx_documents_collection;
DROP TABLE IF EXISTS documents;
```

- [ ] **Step 3: Write PostgreSQL up migration**

```sql
-- migrations/postgres/000001_init.up.sql

CREATE TABLE IF NOT EXISTS documents (
    path        TEXT        PRIMARY KEY,
    collection  TEXT        NOT NULL,
    parent      TEXT,
    data        JSONB,
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    version     BIGINT      NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_documents_collection ON documents(collection);
CREATE INDEX IF NOT EXISTS idx_documents_parent     ON documents(parent);
CREATE INDEX IF NOT EXISTS idx_documents_updated_at ON documents(updated_at);

-- GIN index on data enables efficient JSONB field lookups
CREATE INDEX IF NOT EXISTS idx_documents_data ON documents USING GIN(data);

CREATE TABLE IF NOT EXISTS indexes (
    id          TEXT    PRIMARY KEY,
    collection  TEXT    NOT NULL,
    fields      JSONB   NOT NULL, -- [{field, order}]
    state       TEXT    NOT NULL DEFAULT 'READY'
);

CREATE TABLE IF NOT EXISTS transactions (
    id          TEXT        PRIMARY KEY,
    started_at  TIMESTAMPTZ NOT NULL,
    reads       JSONB,
    expires_at  TIMESTAMPTZ NOT NULL
);
```

- [ ] **Step 4: Write PostgreSQL down migration**

```sql
-- migrations/postgres/000001_init.down.sql

DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS indexes;
DROP INDEX IF EXISTS idx_documents_data;
DROP INDEX IF EXISTS idx_documents_updated_at;
DROP INDEX IF EXISTS idx_documents_parent;
DROP INDEX IF EXISTS idx_documents_collection;
DROP TABLE IF EXISTS documents;
```

- [ ] **Step 5: Commit**

```bash
git add migrations/
git commit -m "feat: add initial schema migrations for SQLite and PostgreSQL"
```

---

## Task 6: SQLite Adapter (Ping, Close, Migrate)

**Files:**
- Create: `internal/store/sqlite/sqlite.go`
- Create: `internal/store/sqlite/sqlite_test.go`

- [ ] **Step 1: Install dependencies**

```bash
go get modernc.org/sqlite
go get github.com/golang-migrate/migrate/v4
go get github.com/golang-migrate/migrate/v4/database/sqlite
go get github.com/golang-migrate/migrate/v4/source/file
go mod tidy
```

- [ ] **Step 2: Write the failing test**

```go
// internal/store/sqlite/sqlite_test.go
package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/embyr/embyr/internal/store/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLiteAdapter_PingAndMigrate(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	migrationsPath := "../../../migrations/sqlite"

	adapter, err := sqlite.New(dbPath, migrationsPath)
	require.NoError(t, err)
	defer adapter.Close()

	ctx := context.Background()

	err = adapter.Migrate(ctx)
	require.NoError(t, err, "migrations should run without error")

	err = adapter.Ping(ctx)
	require.NoError(t, err, "ping should succeed after migration")
}

func TestSQLiteAdapter_MigrateIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	migrationsPath := "../../../migrations/sqlite"

	adapter, err := sqlite.New(dbPath, migrationsPath)
	require.NoError(t, err)
	defer adapter.Close()

	ctx := context.Background()

	require.NoError(t, adapter.Migrate(ctx))
	err = adapter.Migrate(ctx)
	assert.NoError(t, err, "running migrations twice should not error")
}
```

- [ ] **Step 3: Run tests to confirm they fail**

```bash
go test ./internal/store/sqlite/... -v
```

Expected: compile error — `sqlite` package does not exist.

- [ ] **Step 4: Implement the SQLite adapter**

```go
// internal/store/sqlite/sqlite.go
package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	migratesqlite "github.com/golang-migrate/migrate/v4/database/sqlite"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "modernc.org/sqlite"
)

// Adapter implements store.StorageAdapter for SQLite.
// Additional methods (document ops, queries, listeners) are added in Plans 2–4.
type Adapter struct {
	db             *sql.DB
	migrationsPath string
}

// New opens (or creates) a SQLite database at path and returns a ready Adapter.
func New(path, migrationsPath string) (*Adapter, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open %s: %w", path, err)
	}
	// SQLite is not safe for concurrent writers without WAL mode.
	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: enable WAL: %w", err)
	}
	return &Adapter{db: db, migrationsPath: migrationsPath}, nil
}

// Ping verifies the database connection is alive.
func (a *Adapter) Ping(ctx context.Context) error {
	return a.db.PingContext(ctx)
}

// Migrate applies all pending up-migrations from migrationsPath.
func (a *Adapter) Migrate(ctx context.Context) error {
	driver, err := migratesqlite.WithInstance(a.db, &migratesqlite.Config{})
	if err != nil {
		return fmt.Errorf("sqlite: migrate driver: %w", err)
	}
	m, err := migrate.NewWithDatabaseInstance(
		"file://"+a.migrationsPath,
		"sqlite", driver,
	)
	if err != nil {
		return fmt.Errorf("sqlite: migrate init: %w", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("sqlite: migrate up: %w", err)
	}
	return nil
}

// Close releases the database connection pool.
func (a *Adapter) Close() error {
	return a.db.Close()
}
```

- [ ] **Step 5: Run tests to confirm they pass**

```bash
go test ./internal/store/sqlite/... -v
```

Expected:
```
--- PASS: TestSQLiteAdapter_PingAndMigrate (0.01s)
--- PASS: TestSQLiteAdapter_MigrateIsIdempotent (0.01s)
PASS
```

- [ ] **Step 6: Commit**

```bash
git add internal/store/sqlite/ go.mod go.sum
git commit -m "feat: add SQLite adapter with Ping, Close, and Migrate"
```

---

## Task 7: PostgreSQL Adapter (Ping, Close, Migrate)

**Files:**
- Create: `internal/store/postgres/postgres.go`
- Create: `internal/store/postgres/postgres_test.go`

Tests are skipped automatically when `TEST_POSTGRES_DSN` is not set, so CI without a database still passes.

- [ ] **Step 1: Install dependencies**

```bash
go get github.com/jackc/pgx/v5
go get github.com/jackc/pgx/v5/stdlib
go get github.com/golang-migrate/migrate/v4/database/postgres
go mod tidy
```

- [ ] **Step 2: Write the failing test**

```go
// internal/store/postgres/postgres_test.go
package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/embyr/embyr/internal/store/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dsn(t *testing.T) string {
	t.Helper()
	v := os.Getenv("TEST_POSTGRES_DSN")
	if v == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skipping PostgreSQL tests")
	}
	return v
}

func TestPostgresAdapter_PingAndMigrate(t *testing.T) {
	migrationsPath := "../../../migrations/postgres"

	adapter, err := postgres.New(dsn(t), migrationsPath)
	require.NoError(t, err)
	defer adapter.Close()

	ctx := context.Background()

	err = adapter.Migrate(ctx)
	require.NoError(t, err, "migrations should run without error")

	err = adapter.Ping(ctx)
	require.NoError(t, err, "ping should succeed after migration")
}

func TestPostgresAdapter_MigrateIsIdempotent(t *testing.T) {
	migrationsPath := "../../../migrations/postgres"

	adapter, err := postgres.New(dsn(t), migrationsPath)
	require.NoError(t, err)
	defer adapter.Close()

	ctx := context.Background()

	require.NoError(t, adapter.Migrate(ctx))
	err = adapter.Migrate(ctx)
	assert.NoError(t, err, "running migrations twice should not error")
}
```

- [ ] **Step 3: Run tests to confirm they are skipped (not failed)**

```bash
go test ./internal/store/postgres/... -v
```

Expected:
```
--- SKIP: TestPostgresAdapter_PingAndMigrate (0.00s)
--- SKIP: TestPostgresAdapter_MigrateIsIdempotent (0.00s)
PASS
```

(Compile error expected if package not yet written — that's the intended red state.)

- [ ] **Step 4: Implement the PostgreSQL adapter**

```go
// internal/store/postgres/postgres.go
package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	migratepostgres "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Adapter implements store.StorageAdapter for PostgreSQL.
// Additional methods (document ops, queries, listeners) are added in Plans 2–4.
type Adapter struct {
	db             *sql.DB
	migrationsPath string
}

// New opens a connection pool to the PostgreSQL database at dsn and returns a ready Adapter.
func New(dsn, migrationsPath string) (*Adapter, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: open: %w", err)
	}
	return &Adapter{db: db, migrationsPath: migrationsPath}, nil
}

// Ping verifies the database connection is alive.
func (a *Adapter) Ping(ctx context.Context) error {
	return a.db.PingContext(ctx)
}

// Migrate applies all pending up-migrations from migrationsPath.
func (a *Adapter) Migrate(ctx context.Context) error {
	driver, err := migratepostgres.WithInstance(a.db, &migratepostgres.Config{})
	if err != nil {
		return fmt.Errorf("postgres: migrate driver: %w", err)
	}
	m, err := migrate.NewWithDatabaseInstance(
		"file://"+a.migrationsPath,
		"postgres", driver,
	)
	if err != nil {
		return fmt.Errorf("postgres: migrate init: %w", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("postgres: migrate up: %w", err)
	}
	return nil
}

// Close releases the database connection pool.
func (a *Adapter) Close() error {
	return a.db.Close()
}
```

- [ ] **Step 5: Run tests (skip expected without DSN; pass with DSN)**

```bash
go test ./internal/store/postgres/... -v
```

Expected without `TEST_POSTGRES_DSN`:
```
--- SKIP: TestPostgresAdapter_PingAndMigrate (0.00s)
--- SKIP: TestPostgresAdapter_MigrateIsIdempotent (0.00s)
PASS
```

Expected with `TEST_POSTGRES_DSN=postgres://user:pass@localhost/testdb`:
```
--- PASS: TestPostgresAdapter_PingAndMigrate (0.05s)
--- PASS: TestPostgresAdapter_MigrateIsIdempotent (0.04s)
PASS
```

- [ ] **Step 6: Commit**

```bash
git add internal/store/postgres/ go.mod go.sum
git commit -m "feat: add PostgreSQL adapter with Ping, Close, and Migrate"
```

---

## Task 8: Health Endpoints

**Files:**
- Create: `internal/health/health.go`
- Create: `internal/health/health_test.go`

- [ ] **Step 1: Write the failing tests**

```go
// internal/health/health_test.go
package health_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/embyr/embyr/internal/health"
	"github.com/stretchr/testify/assert"
)

type mockAdapter struct{ pingErr error }

func (m *mockAdapter) Ping(_ context.Context) error  { return m.pingErr }
func (m *mockAdapter) Migrate(_ context.Context) error { return nil }
func (m *mockAdapter) Close() error                   { return nil }

func TestHealthz(t *testing.T) {
	h := health.New(&mockAdapter{})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	h.Healthz(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "ok", w.Body.String())
}

func TestReadyz_DBReachable(t *testing.T) {
	h := health.New(&mockAdapter{})
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	h.Readyz(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "ok", w.Body.String())
}

func TestReadyz_DBUnreachable(t *testing.T) {
	h := health.New(&mockAdapter{pingErr: errors.New("connection refused")})
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	h.Readyz(w, req)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
go test ./internal/health/... -v
```

Expected: compile error — `health` package does not exist.

- [ ] **Step 3: Implement health.go**

```go
// internal/health/health.go
package health

import (
	"context"
	"net/http"
	"time"
)

// Pinger is the subset of store.StorageAdapter needed by health checks.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Handler holds the health and readiness HTTP handlers.
type Handler struct {
	db Pinger
}

// New returns a Handler that uses db for readiness checks.
func New(db Pinger) *Handler {
	return &Handler{db: db}
}

// Healthz always returns 200 OK — the process is alive.
func (h *Handler) Healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// Readyz returns 200 OK when the database is reachable, 503 otherwise.
func (h *Handler) Readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.db.Ping(ctx); err != nil {
		http.Error(w, "db unreachable: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
go test ./internal/health/... -v
```

Expected:
```
--- PASS: TestHealthz (0.00s)
--- PASS: TestReadyz_DBReachable (0.00s)
--- PASS: TestReadyz_DBUnreachable (0.00s)
PASS
```

- [ ] **Step 5: Commit**

```bash
git add internal/health/
git commit -m "feat: add /healthz and /readyz HTTP handlers"
```

---

## Task 9: gRPC Server Scaffold with grpc-gateway REST Mux

**Files:**
- Create: `internal/server/server.go`
- Create: `internal/server/server_test.go`

- [ ] **Step 1: Install zap**

```bash
go get go.uber.org/zap
go mod tidy
```

- [ ] **Step 2: Write the failing test**

```go
// internal/server/server_test.go
package server_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/embyr/embyr/internal/config"
	"github.com/embyr/embyr/internal/server"
	"github.com/embyr/embyr/internal/store/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func TestServer_StartsAndServes(t *testing.T) {
	dir := t.TempDir()
	adapter, err := sqlite.New(dir+"/test.db", "../../../migrations/sqlite")
	require.NoError(t, err)
	require.NoError(t, adapter.Migrate(context.Background()))
	defer adapter.Close()

	grpcPort := freePort(t)
	restPort := freePort(t)

	cfg := &config.Config{}
	cfg.Server.GRPCPort = grpcPort
	cfg.Server.RESTPort = restPort

	log, _ := zap.NewDevelopment()
	srv, err := server.New(cfg, adapter, log)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run(ctx) }()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// REST health check via the REST port (served on the same mux as grpc-gateway)
	resp, err := http.Get("http://127.0.0.1:" + itoa(restPort) + "/healthz")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	cancel()
	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop within 2s")
	}
}

func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}
```

- [ ] **Step 3: Run test to confirm it fails**

```bash
go test ./internal/server/... -v
```

Expected: compile error — `server` package does not exist.

- [ ] **Step 4: Implement server.go**

```go
// internal/server/server.go
package server

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/embyr/embyr/internal/config"
	"github.com/embyr/embyr/internal/health"
	"github.com/embyr/embyr/internal/store"
	firestorev1 "github.com/embyr/embyr/gen/go/google/firestore/v1"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// firestoreServer is the gRPC service implementation.
// All RPCs return UNIMPLEMENTED until Plans 2–4 fill them in.
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
	health     *health.Handler
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
	// Register the gateway pointing at our own gRPC server.
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
		health:     h,
	}, nil
}

// Run starts both the gRPC and REST listeners. It blocks until ctx is cancelled.
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

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return s.grpcServer.Serve(grpcLis) })
	eg.Go(func() error { return restSrv.Serve(restLis) })
	eg.Go(func() error {
		<-ctx.Done()
		s.grpcServer.GracefulStop()
		return restSrv.Shutdown(context.Background())
	})
	return eg.Wait()
}

func loggingUnaryInterceptor(log *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		code := status.Code(err)
		log.Info("rpc", zap.String("method", info.FullMethod), zap.String("code", code.String()))
		return resp, err
	}
}

func loggingStreamInterceptor(log *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		code := status.Code(err)
		log.Info("rpc_stream", zap.String("method", info.FullMethod), zap.String("code", code.String()))
		return err
	}
}
```

- [ ] **Step 5: Install errgroup**

```bash
go get golang.org/x/sync
go mod tidy
```

- [ ] **Step 6: Run tests to confirm they pass**

```bash
go test ./internal/server/... -v -timeout 10s
```

Expected:
```
--- PASS: TestServer_StartsAndServes (0.15s)
PASS
```

- [ ] **Step 7: Commit**

```bash
git add internal/server/ go.mod go.sum
git commit -m "feat: add gRPC + grpc-gateway REST server scaffold"
```

---

## Task 10: Binary Entry Point

**Files:**
- Create: `cmd/embyr/main.go`

- [ ] **Step 1: Write main.go**

```go
// cmd/embyr/main.go
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/embyr/embyr/internal/config"
	"github.com/embyr/embyr/internal/server"
	"github.com/embyr/embyr/internal/store"
	"github.com/embyr/embyr/internal/store/postgres"
	"github.com/embyr/embyr/internal/store/sqlite"
	"go.uber.org/zap"
)

func main() {
	configPath := flag.String("config", "", "path to config YAML file (optional)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("embyr: load config: %v", err)
	}

	var zapCfg zap.Config
	if cfg.Log.Format == "json" {
		zapCfg = zap.NewProductionConfig()
	} else {
		zapCfg = zap.NewDevelopmentConfig()
	}
	logger, err := zapCfg.Build()
	if err != nil {
		log.Fatalf("embyr: build logger: %v", err)
	}
	defer logger.Sync()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var adapter store.StorageAdapter

	switch cfg.Backend.Type {
	case "sqlite":
		a, err := sqlite.New(cfg.Backend.SQLite.Path, "migrations/sqlite")
		if err != nil {
			logger.Fatal("sqlite: open", zap.Error(err))
		}
		defer a.Close()
		adapter = a
	case "postgres":
		a, err := postgres.New(cfg.Backend.Postgres.DSN, "migrations/postgres")
		if err != nil {
			logger.Fatal("postgres: open", zap.Error(err))
		}
		defer a.Close()
		adapter = a
	default:
		logger.Fatal("unknown backend type", zap.String("type", cfg.Backend.Type))
	}

	if err := adapter.Migrate(ctx); err != nil {
		logger.Fatal("migrate", zap.Error(err))
	}
	logger.Info("migrations applied")

	srv, err := server.New(cfg, adapter, logger)
	if err != nil {
		logger.Fatal("server: new", zap.Error(err))
	}

	logger.Info("embyr starting",
		zap.Int("grpc_port", cfg.Server.GRPCPort),
		zap.Int("rest_port", cfg.Server.RESTPort),
		zap.String("backend", cfg.Backend.Type),
		zap.String("auth", cfg.Auth.Mode),
	)

	if err := srv.Run(ctx); err != nil {
		logger.Fatal("server exited", zap.Error(err))
	}
}
```

- [ ] **Step 2: Build the binary**

```bash
make build
```

Expected: `embyr` binary produced in the project root, exits 0.

- [ ] **Step 3: Smoke test with SQLite backend**

```bash
./embyr --config /dev/null &
sleep 1
curl -s http://localhost:8081/healthz
```

Expected output: `ok`

```bash
curl -s http://localhost:8081/readyz
```

Expected output: `ok`

- [ ] **Step 4: Kill the background process**

```bash
kill %1
```

- [ ] **Step 5: Run the full test suite**

```bash
make test
```

Expected: all tests pass (PostgreSQL tests skipped if no DSN).

- [ ] **Step 6: Final commit**

```bash
git add cmd/embyr/main.go go.mod go.sum
git commit -m "feat: add binary entry point — embyr starts and serves health endpoints"
```

---

## Verification Checklist

Before declaring Plan 1 complete, confirm:

- [ ] `make build` produces a static binary (`file embyr` shows `statically linked`)
- [ ] `go test ./...` passes with zero failures (skips allowed for PostgreSQL)
- [ ] `./embyr` starts, `/healthz` returns `ok`, `/readyz` returns `ok`
- [ ] gRPC server responds (any RPC returns `UNIMPLEMENTED`, not a connection error): `grpcurl -plaintext localhost:8080 list`
- [ ] REST gateway responds: `curl http://localhost:8081/v1/projects/p/databases/d/documents` returns a JSON error (not a connection refused)
- [ ] `buf generate` is repeatable — running it twice produces identical output

---

## Up Next: Plan 2 — Document CRUD + Auth

Plan 2 will implement:
- `GetDocument`, `SetDocument`, `UpdateDocument`, `DeleteDocument`, `ListDocuments` on both adapters
- The four auth interceptors (`none`, `google`, `key`, `mtls`) wired into the server's interceptor chain
- The `StorageAdapter` interface will be extended with document operation methods
