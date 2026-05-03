# Multi-Tenancy Design

**Date:** 2026-05-02  
**Status:** Approved  

## Overview

Embyr is a Firestore-protocol-to-SQL translator SaaS. Customers bring their own PostgreSQL database; Embyr converts the Firestore wire protocol to SQL against it. Embyr never owns customer data.

The current codebase is a single-tenant monolith: one process, one DB adapter, no tenant routing. This design adapts it for multi-tenancy and breaks it into two deployable units: a control plane (Cloud Functions) and a data plane (Cloud Run).

---

## Architecture

### Service Split

```
┌─────────────────────────────────────────────────────────────────┐
│ Dashboard (separate spec, follow-on)                             │
│  Onboarding wizard, tenant management UI, usage overview         │
│  Calls control plane API with user-scoped key                    │
└──────────────────────────┬──────────────────────────────────────┘
                           │ HTTP (user-scoped API key)
┌──────────────────────────▼──────────────────────────────────────┐
│ Control Plane (Cloud Functions)                                  │
│  cmd/control — HTTP server, deployed as CF HTTP function         │
│  User auth: Google OAuth sign-in → issue scoped API key          │
│  Admin auth: static admin key (Embyr ops)                        │
│  • POST /v1/auth/google      exchange OAuth code → user key      │
│  • POST /v1/tenants          register tenant                     │
│  • DELETE /v1/tenants/:id    deregister                          │
│  • GET /v1/tenants           list (own tenants only)             │
│  • PATCH /v1/tenants/:id     update / suspend / reactivate       │
│  • POST /v1/tenants/:id/test test connectivity                   │
│  • POST /v1/tenants/:id/agents       create agent token          │
│  • DELETE /v1/tenants/:id/agents/:id revoke agent                │
│  Runs migrations on customer DB at registration time             │
│  Writes to Registry DB (Neon Postgres, Embyr-owned)              │
└──────────────────────────┬──────────────────────────────────────┘
                           │ writes
                    ┌──────▼──────┐
                    │ Registry DB  │  (Neon Postgres, Embyr-owned)
                    └──────┬──────┘
                           │ reads (60s poll / cache)
┌──────────────────────────▼──────────────────────────────────────┐
│ Data Plane (Cloud Run)                                           │
│  cmd/data — existing Firestore protocol server + tenancy layer   │
│  • gRPC + grpc-gateway REST + WebChannel — unchanged             │
│  • Middleware extracts (projectId, databaseId) from path         │
│  • Resolves StorageAdapter from LRU pool                         │
│  • Injects adapter into context                                  │
│  • Per-tenant connection pool, search_path=embyr_{databaseId}    │
└──────────────────────────┬──────────────────────────────────────┘
                           │ connects to
              ┌────────────┴────────────┐
         ┌────▼────┐              ┌─────▼────┐
         │Tenant A │              │Tenant B  │  (customer-owned)
         │Postgres │              │Postgres  │
         └─────────┘              └──────────┘
```

### Repository Structure

```
cmd/
  control/main.go        -- new: control plane entry point
  data/main.go           -- renamed from cmd/embyr/main.go
agent/                   -- new: Rust crate for embyr-agent binary
  Cargo.toml
  src/
    main.rs
    tunnel.rs
    proxy.rs
    auth.rs
internal/
  registry/              -- new: tenant lookup + cache client
  tenancy/               -- new: adapter factory + LRU pool
  migrations/            -- extracted: schema runner, standalone
  server/                -- existing, unchanged except context plumbing
  store/                 -- existing, unchanged
  codec/                 -- existing, unchanged
  webchannel/            -- existing, unchanged
  listen/                -- existing, unchanged
  auth/                  -- existing, extended for per-tenant config
  config/                -- existing, extended
```

---

## Tenant Registry Schema

Stored in Embyr's own Neon Postgres instance.

```sql
CREATE TABLE users (
    id           TEXT PRIMARY KEY,
    email        TEXT UNIQUE NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE user_identities (
    id               TEXT PRIMARY KEY,
    user_id          TEXT REFERENCES users(id) ON DELETE CASCADE,
    provider         TEXT NOT NULL,   -- google | github | gitlab | bitbucket
    provider_user_id TEXT NOT NULL,
    UNIQUE (provider, provider_user_id)
);

CREATE TABLE user_keys (
    id           TEXT PRIMARY KEY,
    user_id      TEXT REFERENCES users(id) ON DELETE CASCADE,
    key_hash     TEXT NOT NULL,           -- bcrypt of the issued API key
    label        TEXT,                    -- human-readable name
    created_at   TIMESTAMPTZ NOT NULL,
    last_used_at TIMESTAMPTZ
);

CREATE TABLE tenants (
    id              TEXT PRIMARY KEY,
    owner_id        TEXT REFERENCES users(id) ON DELETE RESTRICT,
    project_id      TEXT NOT NULL,
    database_id     TEXT NOT NULL,
    schema_name     TEXT NOT NULL,        -- defaults to embyr_{database_id}
    credential_type TEXT NOT NULL,        -- gcp_secret | aws_secret | embyr_secret | agent
    credential_ref  TEXT NOT NULL,        -- resource name / ARN / encrypted blob / agent_id
    auth_mode       TEXT NOT NULL DEFAULT 'google',
    auth_config     JSONB,                -- {"google_project_id": "..."} | {"key": "..."}
    status          TEXT NOT NULL DEFAULT 'provisioning', -- provisioning | active | suspended
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    UNIQUE (project_id, database_id)
);

CREATE TABLE agents (
    id           TEXT PRIMARY KEY,
    tenant_id    TEXT REFERENCES tenants(id) ON DELETE CASCADE,
    public_key   TEXT NOT NULL,           -- Ed25519 public key, base64-encoded
    last_seen_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL
);
```

---

## Credential Layer

### CredentialResolver Interface

```go
type CredentialResolver interface {
    Resolve(ctx context.Context) (dsn string, err error)
}
```

Implementations (in `internal/tenancy`):

| Type | Implementation | Notes |
|---|---|---|
| `gcp_secret` | `GCPSecretResolver` | Fetches from GCP Secret Manager via resource name |
| `aws_secret` | `AWSSecretResolver` | Fetches from AWS Secrets Manager via ARN |
| `embyr_secret` | `EmbyrSecretResolver` | Decrypts AES-GCM blob from registry, key in Embyr's own Cloud KMS |
| `agent` | `AgentResolver` | Returns tunnel DSN; blocks until agent tunnel is live |

`credential_ref` values:
- `gcp_secret` → `projects/acme/secrets/embyr-dsn/versions/latest`
- `aws_secret` → `arn:aws:secretsmanager:us-east-1:123456789:secret:embyr-dsn`
- `embyr_secret` → base64-encoded AES-GCM ciphertext (key in Cloud KMS)
- `agent` → `agents.id` foreign key

Customers are advised to create a dedicated, limited Postgres user for Embyr scoped only to the `embyr_*` schema. For `gcp_secret` and `aws_secret`, the customer stores the DSN in their own secret store and grants Embyr's service account read access — Embyr never receives or stores the raw credential.

---

## Schema Isolation

Each Firestore database maps to a dedicated PG schema on the customer's Postgres instance:

```
customer's postgres instance
  └── schema: embyr_{databaseId}
        ├── documents
        ├── indexes
        └── transactions
  └── public schema: untouched (customer's own tables)
```

`search_path=embyr_{databaseId}` is set on every connection via pgx `ConnConfig.RuntimeParams`. No changes required to existing SQL queries.

### Migration Runner (`internal/migrations`)

Extracted from `internal/store/postgres`. Each migration file gains a schema-creation preamble rendered at runtime:

```sql
CREATE SCHEMA IF NOT EXISTS {{.Schema}};
SET search_path = {{.Schema}};
-- existing table definitions follow unchanged
```

```go
type Runner struct{}
func (r *Runner) Up(ctx context.Context, db *sql.DB, schemaName string) error
func (r *Runner) Down(ctx context.Context, db *sql.DB, schemaName string) error
```

**Lifecycle:**
- Registration → control plane runs `migrations.Up` → sets `status=active`
- Deregistration → control plane runs `migrations.Down` only if explicitly requested; default leaves data intact
- Data plane never runs migrations

---

## Tenancy Layer (Data Plane)

### AdapterFactory (`internal/tenancy`)

Lazy-init LRU cache of `StorageAdapter` instances keyed by `(projectId, databaseId)`. Default capacity: 200 entries (configurable).

```go
type AdapterFactory struct {
    registry *registry.Client
    cache    *lru.Cache[tenantKey, *entry]
    mu       sync.RWMutex
}

func (f *AdapterFactory) Get(ctx context.Context, projectID, databaseID string) (store.StorageAdapter, *auth.Config, error)
```

On cache miss:
1. Look up tenant in registry cache (refreshed every 60s)
2. Resolve credential via `CredentialResolver`
3. Open Postgres connection pool with `search_path=embyr_{databaseId}`
4. Run health check ping
5. Cache entry

On LRU eviction: close connection pool gracefully.

Suspended tenants → `codes.PermissionDenied` before connection attempt.  
Unknown tenants → `codes.NotFound` before connection attempt.

### Middleware

gRPC unary + stream interceptors and HTTP middleware extract `(projectId, databaseId)` from the incoming Firestore resource path:

```
projects/{projectId}/databases/{databaseId}/documents/...
```

Auth order per request:
1. Extract `(projectId, databaseId)` from path
2. Look up tenant in registry cache
3. Validate token against tenant's `auth_mode` + `auth_config`
4. Resolve `StorageAdapter` from `AdapterFactory`
5. Inject into context
6. Dispatch to handler

Both "unknown tenant" and "invalid token" return `codes.Unauthenticated` with the same generic message — prevents tenant enumeration by unauthenticated callers.

Handlers replace `s.db` with `adapterFromCtx(ctx)`. This is the only change to existing handler code.

---

## Control Plane API

Plain JSON REST, deployed as GCP Cloud Function HTTP trigger.

### Auth Tiers

| Caller | Credential | Scope |
|---|---|---|
| Embyr ops | Static admin key (Secret Manager) | All tenants, all users |
| Customer | User-scoped API key | Own tenants only |

User-scoped keys are issued after Google OAuth sign-in. A user can create multiple named keys (e.g. "CLI", "dashboard") and revoke them independently.

### User Auth Endpoints

Supported OAuth providers: Google, GitHub, GitLab, Bitbucket. All use the same authorization code flow. Identity is keyed by `(provider, provider_user_id)`; email is used to merge accounts across providers (sign in with GitHub and Google using the same email → same user account).

```
POST /v1/auth/:provider        provider = google | github | gitlab | bitbucket
  Body: {"code": "<oauth_authorization_code>", "redirect_uri": "..."}
  → Verifies with provider, upserts user + user_identity rows, issues API key
  → Returns: {"key": "<plaintext, shown once>", "user_id": "..."}

GET    /v1/auth/keys           List own API keys (id + label, no plaintext)
DELETE /v1/auth/keys/:id       Revoke a key
```

### Tenant Endpoints

All tenant write operations are scoped to `owner_id = current user`. Admin key bypasses ownership checks.

```
POST   /v1/tenants              Register tenant (runs migrations, sets status=active)
GET    /v1/tenants              List own tenants
GET    /v1/tenants/:id          Get single tenant
PATCH  /v1/tenants/:id          Update credential_ref / suspend / reactivate
DELETE /v1/tenants/:id          Deregister (optionally drop schema)
POST   /v1/tenants/:id/test     Test connectivity — resolve credential, ping DB
POST   /v1/tenants/:id/agents   Create agent + return one-time private key
DELETE /v1/tenants/:id/agents/:aid  Revoke agent
```

### Registration Flow

```
POST /v1/tenants

Body:
{
  "project_id":       "acme",
  "database_id":      "prod",
  "credential_type":  "gcp_secret",
  "credential_ref":   "projects/acme/secrets/embyr-dsn/versions/latest",
  "schema_name":      "embyr_prod",        // optional
  "auth_mode":        "google",
  "auth_config":      {"google_project_id": "acme-gcp-project"}
}

Steps:
1. Validate request
2. Resolve credential (verify it works before committing)
3. Ping customer DB
4. Run migrations.Up with schema_name
5. Insert tenant row (owner_id = current user, status=active)
6. Return tenant ID + onboarding instructions
```

Response includes per-`credential_type` onboarding instructions (SQL grant statements, IAM binding commands, agent download URL).

---

## embyr-agent (Rust)

Standalone static binary. Customers run it inside their network. Dials outbound to Embyr — no inbound ports required, DB never exposed to the internet.

### Protocol

Agent establishes a persistent TLS connection to `agent.embyr.dev`. Embyr multiplexes SQL connections over this tunnel using length-prefixed frames, each carrying a `connection_id`.

```
Embyr Cloud Run                       Customer network
───────────────                       ───────────────
AgentResolver ──── TLS tunnel ──────▶ embyr-agent ──▶ Postgres:5432
(SQL frames)       (agent dials out)  (localhost or private host)
```

### Authentication

At agent creation, control plane generates an Ed25519 keypair. Private key delivered once to operator, never stored by Embyr. Public key stored in `agents` table. Agent authenticates tunnel with signed handshake using private key.

### Resilience

Agent reconnects with exponential backoff on tunnel drop. `AgentResolver.Resolve()` blocks until tunnel is live (configurable timeout → `codes.Unavailable`).

### Distribution

Single static binary cross-compiled for `linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`. Published to GitHub Releases. Docker image provided.

```
agent/
  Cargo.toml
  src/
    main.rs       -- CLI: embyr-agent --token <jwt> --db-url postgres://...
    tunnel.rs     -- outbound TLS + frame multiplexer
    proxy.rs      -- forwards frames to local Postgres
    auth.rs       -- Ed25519 handshake
```

---

## Auth

### Control Plane

Two tiers:
- **Admin key** — static key in Embyr's Secret Manager, used by Embyr ops. Full access.
- **User-scoped key** — issued after Google OAuth sign-in (`POST /v1/auth/google`). Scoped to that user's own tenants. Multiple keys per user, each revocable independently.

Supported OAuth providers: Google, GitHub, GitLab, Bitbucket. Natural fit for developer-facing SaaS — covers GCP-native customers (Google) and the broader developer tooling ecosystem (GitHub/GitLab/Bitbucket). Zero password management for Embyr.

### Data Plane

Auth is per-tenant. `auth_mode` and `auth_config` stored in the tenant registry. Existing auth implementations (`none`, `key`, `google`, `mtls`) reused unchanged. `AdapterFactory.Get` returns the tenant's resolved `auth.Config` alongside the `StorageAdapter`. Middleware uses it to validate the request token before dispatching.

---

## What Stays Unchanged

- `internal/store/adapter.go` — `StorageAdapter` interface
- `internal/store/postgres/` — all CRUD, query, transaction logic
- `internal/store/sqlite/` — kept for local single-tenant dev
- `internal/server/handlers.go` — all Firestore protocol handlers
- `internal/server/listen.go` — real-time listener logic
- `internal/webchannel/` — BrowserChannel implementation
- `internal/codec/` — query translation
- `internal/listen/` — listener registry
- `gen/` — generated protobuf code
- `migrations/postgres/` — existing files, schema preamble added

The tenancy layer is purely additive. `server.New(cfg, db, log)` signature stays unchanged — single-tenant mode, used by `cmd/embyr` and all existing tests. Multi-tenant mode uses `server.NewMultiTenant(cfg, factory, log)`.

Handlers use a single helper instead of `s.db` directly:

```go
func (s *firestoreServer) adapter(ctx context.Context) store.StorageAdapter {
    if a := adapterFromCtx(ctx); a != nil {
        return a // multi-tenant: injected by middleware
    }
    return s.db  // single-tenant: fallback
}
```

Existing tests require zero changes. `cmd/embyr` single-tenant entry point preserved.

---

## Dashboard (Follow-on Spec)

The dashboard is a separate design and implementation effort. It calls the control plane API using user-scoped keys. Scope:

- **Sign-in** — Google OAuth flow
- **Onboarding wizard** — step-by-step guided setup per `credential_type`:
  - `gcp_secret`: instructions to create a secret + grant Embyr service account accessor
  - `aws_secret`: instructions to create a secret + grant IAM role
  - `embyr_secret`: DSN entry form (Embyr stores it encrypted)
  - `agent`: agent download, token generation, install command
- **Tenant dashboard** — status, connection health, usage metrics
- **Key management** — create/revoke API keys

The backend API contract defined in this spec supports the dashboard without changes.

---

## Component Summary

| Component | Language | Deploy Target |
|---|---|---|
| `cmd/control` | Go | Cloud Functions |
| `cmd/data` | Go | Cloud Run |
| `internal/registry` | Go | shared library |
| `internal/tenancy` | Go | shared library |
| `internal/migrations` | Go | shared library |
| `agent/` | Rust | customer infrastructure |
| Dashboard | TBD (follow-on spec) | Cloud Run / Firebase Hosting |
