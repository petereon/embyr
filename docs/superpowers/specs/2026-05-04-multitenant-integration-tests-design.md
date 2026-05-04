# Multi-Tenant Integration Test Suite — Design Spec

**Goal:** End-to-end integration tests that prove multi-tenant routing works correctly — right tenant DB is selected per request, DSNs are fetched from real AWS and GCP secret stores, and data is isolated across tenants.

**Date:** 2026-05-04

---

## 1. Scope

These tests verify the full runtime path introduced in Plan 1 (Core Tenancy Layer):

- `registry.PostgresClient` looks up the correct tenant from the registry DB
- `AWSSecretResolver` fetches a DSN from AWS Secrets Manager
- `GCPSecretResolver` fetches a DSN from GCP Secret Manager
- `AdapterFactory` opens a `StorageAdapter` backed by the tenant's Postgres schema
- gRPC middleware extracts `(projectId, databaseId)` from the Firestore path and injects the adapter
- Firestore CRUD operations are routed to the correct tenant DB
- Data written to one tenant is not visible to another tenant

**Out of scope:** KMS/Embyr-secret credential type, agent credential type, auth mode validation, browser tests.

---

## 2. Architecture

```
TestMain
  ├── start ministack container (AWS SM + RDS Postgres)
  ├── start GCP Secret Manager emulator container
  ├── create registry Postgres via ministack RDS (localhost:15432)
  ├── create tenant Postgres via ministack RDS (localhost:15433)
  ├── create tenants DDL in registry DB
  ├── run Embyr migrations on tenant DB (schema: embyr_test)
  ├── seed AWS secret (tenant DSN) → secret ARN
  ├── seed GCP secret (tenant DSN) → secret name
  ├── insert two tenant rows in registry
  └── start server.NewMultiTenant in-process (random gRPC port)

Tests
  ├── TestAWSTenant_CRUD
  ├── TestGCPTenant_CRUD
  ├── TestTenantIsolation
  └── TestUnknownTenant_Unauthenticated
```

### Containers

| Container | Image | Ports | Purpose |
|---|---|---|---|
| ministack | `ministackorg/ministack` | 4566 (mapped) | AWS Secrets Manager + RDS |
| gcp-sm | `ghcr.io/blackwell-systems/gcp-secret-manager-emulator:dual` | 9090 (gRPC, mapped) | GCP Secret Manager |

ministack requires the Docker socket (`/var/run/docker.sock`) mounted so it can spin up RDS Postgres containers.

### Postgres instances (via ministack RDS)

| Instance | Host:Port | DB name | Schemas used | Purpose |
|---|---|---|---|---|
| registry | `localhost:15432` | `embyr_registry` | `public` | Tenant config store |
| tenant | `localhost:15433` | `embyr_tenant` | `embyr_aws_test`, `embyr_gcp_test` | Firestore documents |

ministack assigns RDS ports sequentially from `RDS_BASE_PORT` (default 15432). The test waits for each Postgres to accept connections before proceeding. The tenant DB hosts **two separate schemas**, one per tenant, so `TestTenantIsolation` tests genuine schema-level isolation — not just Firestore path namespacing.

### In-process server

`server.NewMultiTenant(cfg, factory, logger)` is called in `TestMain`. It listens on `net.Listen("tcp", "127.0.0.1:0")` (random port). The gRPC port is captured and shared via a package-level variable so individual tests can dial it.

---

## 3. Tenant fixtures

Two tenant rows are inserted into the registry `tenants` table:

| project_id | database_id | credential_type | credential_ref | schema_name | auth_mode |
|---|---|---|---|---|---|
| `aws-proj` | `aws-db` | `aws_secret` | ARN of DSN secret in ministack SM | `embyr_aws_test` | `none` |
| `gcp-proj` | `gcp-db` | `gcp_secret` | Full resource name of DSN secret in GCP emulator | `embyr_gcp_test` | `none` |

Both tenants point at the same Postgres **instance** (`localhost:15433`) but use **different schemas** (`embyr_aws_test` and `embyr_gcp_test`). The DSN stored in each secret is identical (same host/port/dbname) but `appendSearchPath` appends the correct `search_path` for each tenant. Both schemas are fully migrated in `TestMain`. The isolation test verifies that documents written to one schema are not accessible through the other tenant's adapter.

---

## 4. Code changes outside the test package

### `internal/tenancy/gcp.go`

Add `EMBYR_GCP_SECRETMANAGER_EMULATOR_HOST` env var support. When set, the resolver dials the emulator with an insecure gRPC connection instead of using Application Default Credentials:

```go
func (r *GCPSecretResolver) Resolve(ctx context.Context) (string, error) {
    opts := []option.ClientOption{}
    if host := os.Getenv("EMBYR_GCP_SECRETMANAGER_EMULATOR_HOST"); host != "" {
        conn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
        if err != nil {
            return "", fmt.Errorf("tenancy: gcp emulator dial: %w", err)
        }
        opts = append(opts, option.WithGRPCConn(conn))
    }
    client, err := secretmanager.NewClient(ctx, opts...)
    ...
}
```

New imports needed: `"google.golang.org/grpc"`, `"google.golang.org/grpc/credentials/insecure"`, `"google.golang.org/api/option"`, `"os"`.

### `internal/tenancy/aws.go`

No changes. AWS SDK v2 reads `AWS_ENDPOINT_URL` automatically via `config.LoadDefaultConfig`.

---

## 5. Environment variables set by TestMain

| Variable | Value | Used by |
|---|---|---|
| `AWS_ENDPOINT_URL` | `http://localhost:<port>` where `<port>` is the host-mapped port for ministack's 4566 (obtained via `container.MappedPort(ctx, "4566/tcp")`) | `AWSSecretResolver` → AWS SDK |
| `AWS_ACCESS_KEY_ID` | `test` | AWS SDK (ministack ignores credential values) |
| `AWS_SECRET_ACCESS_KEY` | `test` | AWS SDK |
| `AWS_DEFAULT_REGION` | `us-east-1` | AWS SDK (ministack uses for ARN construction) |
| `EMBYR_GCP_SECRETMANAGER_EMULATOR_HOST` | `localhost:<gcp-emulator-mapped-port>` | `GCPSecretResolver` |

All set via `os.Setenv` in `TestMain` (integration tests are not parallel, so global env mutation is safe).

---

## 6. Test cases

### TestAWSTenant_CRUD

1. Build a gRPC Firestore client connected to the in-process server.
2. Call `CreateDocument` in `projects/aws-proj/databases/aws-db/documents/items`.
3. Call `GetDocument` on the returned document path.
4. Assert the returned document data matches what was written.
5. Call `DeleteDocument`.
6. Call `GetDocument` again — assert `codes.NotFound`.

### TestGCPTenant_CRUD

Same as `TestAWSTenant_CRUD` but using `projects/gcp-proj/databases/gcp-db/documents/items`.

### TestTenantIsolation

1. Write doc A to `projects/aws-proj/databases/aws-db/documents/items`.
2. Write doc B to `projects/gcp-proj/databases/gcp-db/documents/items` (same relative path as A, different project).
3. `ListDocuments` on `aws-proj/aws-db/items` — assert only doc A is present.
4. `ListDocuments` on `gcp-proj/gcp-db/items` — assert only doc B is present.

### TestUnknownTenant_Unauthenticated

1. Call `GetDocument` on `projects/unknown/databases/nope/documents/x/y`.
2. Assert the error has gRPC status code `codes.Unauthenticated`.

---

## 7. File layout

```
test/multitenant/
  setup_test.go        — TestMain, container helpers, server start
  multitenant_test.go  — the four test cases
```

No new non-test files except the `gcp.go` modification.

---

## 8. Skip and CI behaviour

**Skip condition:** If Docker is not available (container start returns an error in TestMain), call `m.Run()` with a log message and exit 0. Individual tests are also skipped if the `testServer` package variable is nil.

**Run locally:** `go test ./test/multitenant/... -v -count=1` (requires Docker running).

**CI (GitHub Actions):** Runs on the default `ubuntu-latest` runner, which has Docker available. No `services:` block needed — ministack manages its own Postgres containers via the Docker socket. The workflow step is:
```yaml
- run: go test ./test/multitenant/... -v -count=1 -timeout 120s
```

---

## 9. Registry table DDL (created in test setup, not a production migration)

```sql
CREATE TABLE IF NOT EXISTS tenants (
    id              TEXT        PRIMARY KEY,
    project_id      TEXT        NOT NULL,
    database_id     TEXT        NOT NULL,
    schema_name     TEXT        NOT NULL,
    credential_type TEXT        NOT NULL,
    credential_ref  TEXT        NOT NULL,
    auth_mode       TEXT        NOT NULL DEFAULT 'none',
    auth_config     JSONB,
    status          TEXT        NOT NULL DEFAULT 'active',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, database_id)
);
```

This DDL is intentionally duplicated from what the control plane will manage — the integration test owns its own schema setup and teardown.

---

## 10. Dependencies to add

| Package | Purpose |
|---|---|
| `github.com/testcontainers/testcontainers-go` | Container lifecycle management |
| `github.com/testcontainers/testcontainers-go/wait` | Wait strategies (HTTP health, port open) |
| `github.com/aws/aws-sdk-go-v2/service/rds` | Create ministack RDS instances |
| `github.com/aws/aws-sdk-go-v2/service/secretsmanager` | Already present — seed secrets |
| `cloud.google.com/go/secretmanager/apiv1` | Already present — seed GCP secrets |
| `google.golang.org/grpc/credentials/insecure` | Dial GCP emulator without TLS |
