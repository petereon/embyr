# Multi-Tenant Integration Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an end-to-end integration test suite that spins up ministack (AWS Secrets Manager + RDS Postgres) and the GCP Secret Manager emulator via testcontainers-go, then verifies that multi-tenant routing correctly fetches DSNs from real secret stores and routes Firestore gRPC calls to the right tenant Postgres schema.

**Architecture:** `TestMain` starts two Docker containers (ministack, GCP emulator), creates two RDS Postgres instances via ministack, seeds secrets in both stores, inserts tenant rows into the registry DB, and starts `server.NewMultiTenant` in-process on random ports. Tests use the generated Firestore gRPC client to drive the server. `GCPSecretResolver` gets a small env-var-based emulator override so no ADC is needed. Skip gracefully when Docker is unavailable.

**Tech Stack:** Go testcontainers-go, `ministackorg/ministack` Docker image, `ghcr.io/blackwell-systems/gcp-secret-manager-emulator:dual`, AWS SDK v2 (`rds`, `secretsmanager`), GCP Secret Manager Go client, generated Firestore gRPC stubs.

---

## File Map

**New files:**
- `test/multitenant/setup_test.go` — `TestMain`, container helpers, freePort, waitReady, global test state
- `test/multitenant/multitenant_test.go` — four integration test cases

**Modified files:**
- `go.mod` / `go.sum` — add `testcontainers-go`, `aws-sdk-go-v2/service/rds`, `google.golang.org/api`
- `internal/tenancy/gcp.go` — add `EMBYR_GCP_SECRETMANAGER_EMULATOR_HOST` env-var override

**Unchanged:**
- All other files — zero edits

---

## Task 1: Add testcontainers-go, RDS, and google.golang.org/api dependencies

**Files:**
- Modify: `go.mod`, `go.sum`

- [ ] **Step 1: Add the new packages**

```bash
cd /path/to/.worktrees/multi-tenancy

go get github.com/testcontainers/testcontainers-go@latest
go get github.com/aws/aws-sdk-go-v2/service/rds@latest
go get google.golang.org/api@latest
go mod tidy
```

- [ ] **Step 2: Verify build is clean**

```bash
go build ./...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: add testcontainers-go, rds, and google api dependencies"
```

---

## Task 2: Add GCP Secret Manager emulator support to GCPSecretResolver

**Files:**
- Modify: `internal/tenancy/gcp.go`

- [ ] **Step 1: Read the current file**

Current contents of `internal/tenancy/gcp.go`:

```go
package tenancy

import (
    "context"
    "fmt"

    secretmanager "cloud.google.com/go/secretmanager/apiv1"
    "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
)

type GCPSecretResolver struct {
    resourceName string
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

- [ ] **Step 2: Replace with the emulator-aware version**

Replace the entire file with:

```go
package tenancy

import (
	"context"
	"fmt"
	"os"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GCPSecretResolver fetches a DSN from GCP Secret Manager.
// The customer stores their DSN in their own project and grants
// Embyr's service account secretmanager.secretAccessor on that secret.
type GCPSecretResolver struct {
	resourceName string
}

func (r *GCPSecretResolver) Resolve(ctx context.Context) (string, error) {
	var opts []option.ClientOption
	if host := os.Getenv("EMBYR_GCP_SECRETMANAGER_EMULATOR_HOST"); host != "" {
		conn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return "", fmt.Errorf("tenancy: gcp emulator dial %q: %w", host, err)
		}
		opts = append(opts, option.WithGRPCConn(conn))
	}

	client, err := secretmanager.NewClient(ctx, opts...)
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

- [ ] **Step 3: Compile check and existing tests**

```bash
go build ./internal/tenancy/...
go test ./internal/tenancy/... -v -count=1
```

Expected: compile clean, all existing tenancy tests pass.

- [ ] **Step 4: Commit**

```bash
git add internal/tenancy/gcp.go
git commit -m "feat(tenancy): EMBYR_GCP_SECRETMANAGER_EMULATOR_HOST override for GCPSecretResolver"
```

---

## Task 3: Write test/multitenant/setup_test.go

**Files:**
- Create: `test/multitenant/setup_test.go`

This file owns `TestMain` plus all shared setup helpers and package-level state used by the test cases.

- [ ] **Step 1: Create the directory**

```bash
mkdir -p test/multitenant
```

- [ ] **Step 2: Write the file**

```go
package multitenant_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/petereon/embyr/internal/config"
	"github.com/petereon/embyr/internal/migrations"
	"github.com/petereon/embyr/internal/registry"
	"github.com/petereon/embyr/internal/server"
	"github.com/petereon/embyr/internal/tenancy"
)

// Package-level state set up in TestMain and read by individual tests.
var (
	testGRPCAddr string      // e.g. "127.0.0.1:54321"
	testCancelFn func()      // cancel the server context on teardown
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// ── 1. Start ministack ───────────────────────────────────────────────────
	ministackCtr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ministackorg/ministack",
			ExposedPorts: []string{"4566/tcp"},
			Env:          map[string]string{"RDS_BASE_PORT": "15432"},
			Mounts: testcontainers.ContainerMounts{
				testcontainers.BindMount("/var/run/docker.sock", "/var/run/docker.sock"),
			},
			WaitingFor: wait.ForHTTP("/_ministack/health").WithPort("4566/tcp"),
		},
		Started: true,
	})
	if err != nil {
		log.Printf("ministack unavailable (Docker not running?): %v — skipping integration tests", err)
		os.Exit(m.Run()) // runs zero tests; exits 0
	}
	defer ministackCtr.Terminate(ctx) //nolint:errcheck

	ministackPort, _ := ministackCtr.MappedPort(ctx, "4566/tcp")
	ministackURL := "http://localhost:" + ministackPort.Port()

	// Wire the AWS SDK to ministack for the rest of this process.
	os.Setenv("AWS_ENDPOINT_URL", ministackURL)
	os.Setenv("AWS_ACCESS_KEY_ID", "test")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	os.Setenv("AWS_DEFAULT_REGION", "us-east-1")

	// ── 2. Start GCP Secret Manager emulator ────────────────────────────────
	gcpCtr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ghcr.io/blackwell-systems/gcp-secret-manager-emulator:dual",
			ExposedPorts: []string{"9090/tcp"},
			WaitingFor:   wait.ForListeningPort("9090/tcp"),
		},
		Started: true,
	})
	if err != nil {
		log.Printf("gcp emulator unavailable: %v — skipping integration tests", err)
		os.Exit(m.Run())
	}
	defer gcpCtr.Terminate(ctx) //nolint:errcheck

	gcpPort, _ := gcpCtr.MappedPort(ctx, "9090/tcp")
	gcpAddr := "localhost:" + gcpPort.Port()
	os.Setenv("EMBYR_GCP_SECRETMANAGER_EMULATOR_HOST", gcpAddr)

	// ── 3. Build AWS SDK config (picks up env vars set above) ────────────────
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("aws config: %v", err)
	}

	// ── 4. Create two RDS Postgres instances via ministack ───────────────────
	rdsClient := rds.NewFromConfig(awsCfg)

	for _, inst := range []struct{ id, db string }{
		{"embyr-registry", "embyr_registry"},
		{"embyr-tenant", "embyr_tenant"},
	} {
		_, err := rdsClient.CreateDBInstance(ctx, &rds.CreateDBInstanceInput{
			DBInstanceIdentifier: aws.String(inst.id),
			DBInstanceClass:      aws.String("db.t3.micro"),
			Engine:               aws.String("postgres"),
			MasterUsername:       aws.String("postgres"),
			MasterUserPassword:   aws.String("postgres"),
			DBName:               aws.String(inst.db),
			AllocatedStorage:     aws.Int32(20),
		})
		if err != nil {
			log.Fatalf("create RDS instance %q: %v", inst.id, err)
		}
	}

	// ── 5. Discover RDS endpoints and wait for Postgres to accept connections ─
	registryDSN := waitForRDS(ctx, rdsClient, "embyr-registry", "embyr_registry")
	tenantDSN   := waitForRDS(ctx, rdsClient, "embyr-tenant",   "embyr_tenant")

	// ── 6. Set up registry schema ────────────────────────────────────────────
	regDB, err := sql.Open("pgx", registryDSN)
	if err != nil {
		log.Fatalf("open registry db: %v", err)
	}
	defer regDB.Close()

	_, err = regDB.ExecContext(ctx, `
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
		)`)
	if err != nil {
		log.Fatalf("create tenants table: %v", err)
	}

	// ── 7. Run Embyr migrations on both tenant schemas ────────────────────────
	tenantDB, err := sql.Open("pgx", tenantDSN)
	if err != nil {
		log.Fatalf("open tenant db: %v", err)
	}
	defer tenantDB.Close()

	runner := migrations.NewRunner("../../migrations/postgres")
	for _, schema := range []string{"embyr_aws_test", "embyr_gcp_test"} {
		if err := runner.Up(ctx, tenantDB, schema); err != nil {
			log.Fatalf("migrate schema %q: %v", schema, err)
		}
	}

	// ── 8. Seed AWS secret ───────────────────────────────────────────────────
	smClient := secretsmanager.NewFromConfig(awsCfg)
	awsOut, err := smClient.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String("embyr-aws-tenant-dsn"),
		SecretString: aws.String(tenantDSN),
	})
	if err != nil {
		log.Fatalf("create aws secret: %v", err)
	}
	awsSecretARN := aws.ToString(awsOut.ARN)

	// ── 9. Seed GCP secret ───────────────────────────────────────────────────
	gcpConn, err := grpc.NewClient(gcpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial gcp emulator: %v", err)
	}
	gcpSeedClient, err := secretmanager.NewClient(ctx, option.WithGRPCConn(gcpConn))
	if err != nil {
		log.Fatalf("gcp seed client: %v", err)
	}
	defer gcpSeedClient.Close()

	gcpSecret, err := gcpSeedClient.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   "projects/embyr-test",
		SecretId: "embyr-gcp-tenant-dsn",
		Secret: &secretmanagerpb.Secret{
			Replication: &secretmanagerpb.Replication{
				Replication: &secretmanagerpb.Replication_Automatic_{
					Automatic: &secretmanagerpb.Replication_Automatic{},
				},
			},
		},
	})
	if err != nil {
		log.Fatalf("create gcp secret: %v", err)
	}
	if _, err := gcpSeedClient.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent: gcpSecret.Name,
		Payload: &secretmanagerpb.SecretPayload{
			Data: []byte(tenantDSN),
		},
	}); err != nil {
		log.Fatalf("add gcp secret version: %v", err)
	}
	gcpSecretRef := gcpSecret.Name + "/versions/latest"

	// ── 10. Insert tenant rows into registry ──────────────────────────────────
	for _, row := range []struct {
		id, projectID, databaseID, schema, credType, credRef string
	}{
		{"aws-tenant-1", "aws-proj", "aws-db", "embyr_aws_test", "aws_secret", awsSecretARN},
		{"gcp-tenant-1", "gcp-proj", "gcp-db", "embyr_gcp_test", "gcp_secret", gcpSecretRef},
	} {
		_, err := regDB.ExecContext(ctx, `
			INSERT INTO tenants
				(id, project_id, database_id, schema_name, credential_type, credential_ref, auth_mode, auth_config, status)
			VALUES ($1,$2,$3,$4,$5,$6,'none','{}','active')`,
			row.id, row.projectID, row.databaseID, row.schema, row.credType, row.credRef)
		if err != nil {
			log.Fatalf("insert tenant %q: %v", row.id, err)
		}
	}

	// ── 11. Start server.NewMultiTenant in-process ───────────────────────────
	grpcPort := freePort()
	restPort := freePort()

	cfg := &config.Config{
		Server: config.ServerConfig{
			GRPCPort: grpcPort,
			RESTPort: restPort,
		},
	}

	regClient := registry.NewPostgresClient(regDB, 60*time.Second)
	defer regClient.Close()

	factory := tenancy.NewAdapterFactory(regClient, 10).WithLogger(zap.NewNop())
	defer factory.Close()

	srv, err := server.NewMultiTenant(cfg, factory, zap.NewNop())
	if err != nil {
		log.Fatalf("NewMultiTenant: %v", err)
	}

	srvCtx, cancel := context.WithCancel(context.Background())
	testCancelFn = cancel
	go srv.Run(srvCtx) //nolint:errcheck

	if err := waitReady(restPort, 15*time.Second); err != nil {
		log.Fatalf("server not ready: %v", err)
	}

	testGRPCAddr = fmt.Sprintf("127.0.0.1:%d", grpcPort)

	// ── 12. Run tests, then teardown ─────────────────────────────────────────
	code := m.Run()
	cancel()
	os.Exit(code)
}

// waitForRDS polls DescribeDBInstances until the instance is available,
// then blocks until the Postgres port actually accepts connections.
// Returns a DSN for the instance.
func waitForRDS(ctx context.Context, client *rds.Client, instanceID, dbName string) string {
	deadline := time.Now().Add(120 * time.Second)
	var addr string
	for time.Now().Before(deadline) {
		desc, err := client.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: aws.String(instanceID),
		})
		if err == nil && len(desc.DBInstances) > 0 {
			inst := desc.DBInstances[0]
			if aws.ToString(inst.DBInstanceStatus) == "available" && inst.Endpoint != nil {
				addr = fmt.Sprintf("%s:%d", aws.ToString(inst.Endpoint.Address), inst.Endpoint.Port)
				break
			}
		}
		time.Sleep(2 * time.Second)
	}
	if addr == "" {
		log.Fatalf("RDS instance %q not available after 120s", instanceID)
	}

	dsn := fmt.Sprintf("postgres://postgres:postgres@%s/%s?sslmode=disable", addr, dbName)
	// Wait for the Postgres process itself to be ready.
	connDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(connDeadline) {
		db, err := sql.Open("pgx", dsn)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = db.PingContext(pingCtx)
			cancel()
			db.Close()
			if err == nil {
				return dsn
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	log.Fatalf("postgres at %s not ready after 30s", addr)
	return ""
}

// freePort finds an available TCP port on localhost.
func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// waitReady polls /healthz until it returns 200 or timeout.
func waitReady(restPort int, timeout time.Duration) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/healthz", restPort)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:noctx
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("server on :%d not ready after %v", restPort, timeout)
}
```

- [ ] **Step 3: Verify the file compiles (no test run yet — containers needed)**

```bash
go build ./test/multitenant/...
```

Expected: no compile errors. (If `go build` complains about test files, use `go vet ./test/multitenant/...` instead — `go build` on `_test.go`-only packages can emit a "no non-test Go files" message, which is fine.)

```bash
go vet ./test/multitenant/...
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add test/multitenant/setup_test.go
git commit -m "test(multitenant): TestMain with ministack+GCP containers, RDS setup, secret seeding"
```

---

## Task 4: Write test/multitenant/multitenant_test.go

**Files:**
- Create: `test/multitenant/multitenant_test.go`

- [ ] **Step 1: Write the file**

```go
package multitenant_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	firestorev1 "github.com/petereon/embyr/gen/go/google/firestore/v1"
)

// firestoreClient dials the in-process server and returns a Firestore gRPC client.
func firestoreClient(t *testing.T) firestorev1.FirestoreClient {
	t.Helper()
	if testGRPCAddr == "" {
		t.Skip("integration setup did not complete (Docker unavailable?)")
	}
	conn, err := grpc.NewClient(testGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return firestorev1.NewFirestoreClient(conn)
}

// strVal is a convenience constructor for a Firestore string Value.
func strVal(s string) *firestorev1.Value {
	return &firestorev1.Value{ValueType: &firestorev1.Value_StringValue{StringValue: s}}
}

// TestAWSTenant_CRUD verifies that a tenant whose DSN is stored in AWS Secrets
// Manager can write and read documents, and that delete removes them.
func TestAWSTenant_CRUD(t *testing.T) {
	ctx := context.Background()
	fs := firestoreClient(t)

	parent := "projects/aws-proj/databases/aws-db/documents"

	// Create
	created, err := fs.CreateDocument(ctx, &firestorev1.CreateDocumentRequest{
		Parent:       parent,
		CollectionId: "items",
		DocumentId:   "aws-doc-1",
		Document: &firestorev1.Document{
			Fields: map[string]*firestorev1.Value{
				"name": strVal("Alice"),
				"src":  strVal("aws"),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created)

	// Get — data must round-trip
	got, err := fs.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: created.Name})
	require.NoError(t, err)
	assert.Equal(t, "Alice", got.Fields["name"].GetStringValue())
	assert.Equal(t, "aws", got.Fields["src"].GetStringValue())

	// Delete
	_, err = fs.DeleteDocument(ctx, &firestorev1.DeleteDocumentRequest{Name: created.Name})
	require.NoError(t, err)

	// Get after delete — must be NotFound
	_, err = fs.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: created.Name})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

// TestGCPTenant_CRUD verifies the same flow for a tenant whose DSN is stored in
// GCP Secret Manager (via the local emulator).
func TestGCPTenant_CRUD(t *testing.T) {
	ctx := context.Background()
	fs := firestoreClient(t)

	parent := "projects/gcp-proj/databases/gcp-db/documents"

	created, err := fs.CreateDocument(ctx, &firestorev1.CreateDocumentRequest{
		Parent:       parent,
		CollectionId: "items",
		DocumentId:   "gcp-doc-1",
		Document: &firestorev1.Document{
			Fields: map[string]*firestorev1.Value{
				"name": strVal("Bob"),
				"src":  strVal("gcp"),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created)

	got, err := fs.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: created.Name})
	require.NoError(t, err)
	assert.Equal(t, "Bob", got.Fields["name"].GetStringValue())
	assert.Equal(t, "gcp", got.Fields["src"].GetStringValue())

	_, err = fs.DeleteDocument(ctx, &firestorev1.DeleteDocumentRequest{Name: created.Name})
	require.NoError(t, err)

	_, err = fs.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: created.Name})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

// TestTenantIsolation writes documents to both tenants and verifies that listing
// one tenant's collection does not return the other tenant's documents.
// Both tenants share the same Postgres instance but use different schemas
// (embyr_aws_test vs embyr_gcp_test), so this exercises real schema isolation.
func TestTenantIsolation(t *testing.T) {
	ctx := context.Background()
	fs := firestoreClient(t)

	awsParent := "projects/aws-proj/databases/aws-db/documents"
	gcpParent := "projects/gcp-proj/databases/gcp-db/documents"

	// Write one doc to each tenant in the same collection name.
	awsDoc, err := fs.CreateDocument(ctx, &firestorev1.CreateDocumentRequest{
		Parent:       awsParent,
		CollectionId: "shared-col",
		DocumentId:   "isolation-aws",
		Document: &firestorev1.Document{
			Fields: map[string]*firestorev1.Value{"tenant": strVal("aws")},
		},
	})
	require.NoError(t, err)

	gcpDoc, err := fs.CreateDocument(ctx, &firestorev1.CreateDocumentRequest{
		Parent:       gcpParent,
		CollectionId: "shared-col",
		DocumentId:   "isolation-gcp",
		Document: &firestorev1.Document{
			Fields: map[string]*firestorev1.Value{"tenant": strVal("gcp")},
		},
	})
	require.NoError(t, err)

	// List the AWS tenant's collection — should contain only the AWS doc.
	awsList, err := fs.ListDocuments(ctx, &firestorev1.ListDocumentsRequest{
		Parent:       awsParent,
		CollectionId: "shared-col",
	})
	require.NoError(t, err)
	awsNames := docNames(awsList.Documents)
	assert.Contains(t, awsNames, awsDoc.Name)
	assert.NotContains(t, awsNames, gcpDoc.Name)

	// List the GCP tenant's collection — should contain only the GCP doc.
	gcpList, err := fs.ListDocuments(ctx, &firestorev1.ListDocumentsRequest{
		Parent:       gcpParent,
		CollectionId: "shared-col",
	})
	require.NoError(t, err)
	gcpNames := docNames(gcpList.Documents)
	assert.Contains(t, gcpNames, gcpDoc.Name)
	assert.NotContains(t, gcpNames, awsDoc.Name)

	// Cleanup
	fs.DeleteDocument(ctx, &firestorev1.DeleteDocumentRequest{Name: awsDoc.Name}) //nolint:errcheck
	fs.DeleteDocument(ctx, &firestorev1.DeleteDocumentRequest{Name: gcpDoc.Name}) //nolint:errcheck
}

// TestUnknownTenant_Unauthenticated verifies that requests for an unknown
// (project, database) pair return codes.Unauthenticated (not NotFound),
// preventing tenant enumeration.
func TestUnknownTenant_Unauthenticated(t *testing.T) {
	ctx := context.Background()
	fs := firestoreClient(t)

	_, err := fs.GetDocument(ctx, &firestorev1.GetDocumentRequest{
		Name: "projects/unknown/databases/nope/documents/x/y",
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

// docNames extracts the Name field from a slice of documents.
func docNames(docs []*firestorev1.Document) []string {
	names := make([]string, len(docs))
	for i, d := range docs {
		names[i] = d.Name
	}
	return names
}
```

- [ ] **Step 2: Compile check**

```bash
go vet ./test/multitenant/...
```

Expected: no errors.

- [ ] **Step 3: Run the integration tests (requires Docker)**

```bash
go test ./test/multitenant/... -v -count=1 -timeout 180s
```

Expected output (when Docker is available):
```
=== RUN   TestAWSTenant_CRUD
--- PASS: TestAWSTenant_CRUD
=== RUN   TestGCPTenant_CRUD
--- PASS: TestGCPTenant_CRUD
=== RUN   TestTenantIsolation
--- PASS: TestTenantIsolation
=== RUN   TestUnknownTenant_Unauthenticated
--- PASS: TestUnknownTenant_Unauthenticated
PASS
ok  	github.com/petereon/embyr/test/multitenant
```

Expected output (when Docker is NOT available):
```
[ministack unavailable (Docker not running?): ...]
ok  	github.com/petereon/embyr/test/multitenant  0.00s
```

- [ ] **Step 4: Verify the rest of the test suite is unaffected**

```bash
go test ./... -count=1 2>&1 | tail -20
```

Expected: all packages pass. `test/multitenant` passes (or is skipped). No regressions.

- [ ] **Step 5: Commit**

```bash
git add test/multitenant/multitenant_test.go
git commit -m "test(multitenant): end-to-end integration tests for AWS and GCP tenant routing"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task covering it |
|---|---|
| registry.PostgresClient lookup verified | Task 3 — real registry DB, tenant rows inserted, `registry.NewPostgresClient` used in server |
| AWSSecretResolver fetches DSN from AWS SM | Task 3 seeds secret; Task 4 `TestAWSTenant_CRUD` exercises the path end-to-end |
| GCPSecretResolver fetches DSN from GCP SM | Task 2 modifies resolver; Task 3 seeds GCP secret; Task 4 `TestGCPTenant_CRUD` exercises end-to-end |
| AdapterFactory opens StorageAdapter | Task 3 wires factory to server; every CRUD test exercises it |
| gRPC middleware extracts (projectId, databaseId) | Every test uses distinct project/database paths |
| Firestore CRUD routed to correct tenant DB | `TestAWSTenant_CRUD` and `TestGCPTenant_CRUD` |
| Data isolation across tenants | `TestTenantIsolation` — same Postgres instance, different schemas |
| Unknown tenant → codes.Unauthenticated | `TestUnknownTenant_Unauthenticated` |
| Skip when Docker unavailable | Task 3 TestMain — catches container error, logs, exits 0 |
| Individual tests skip if server not started | Task 4 `firestoreClient` helper — `t.Skip` when `testGRPCAddr == ""` |
| Both tenant schemas fully migrated | Task 3 — `runner.Up` called for `embyr_aws_test` and `embyr_gcp_test` |
| Existing tests unaffected | Task 4 Step 4 — run full `go test ./...` |

**No placeholders found.** All steps include exact code or exact commands.

**Type consistency check:**
- `testGRPCAddr string` declared in `setup_test.go`, read in `multitenant_test.go` via `firestoreClient` helper ✓
- `testCancelFn func()` declared in `setup_test.go`, called on teardown ✓  
- `firestorev1.CreateDocumentRequest.Parent` = `"projects/aws-proj/databases/aws-db/documents"` matches `ParseTenantKey` expectation (`projects/{p}/databases/{d}/...`) ✓
- `migrations.NewRunner("../../migrations/postgres")` — path relative to `test/multitenant/`, resolves to `migrations/postgres/` at repo root ✓
- `registry.NewPostgresClient(regDB, 60*time.Second)` — matches signature in `internal/registry/client.go` ✓
- `tenancy.NewAdapterFactory(regClient, 10).WithLogger(zap.NewNop())` — matches `factory.go` ✓
- `server.NewMultiTenant(cfg, factory, zap.NewNop())` — matches `server.go` ✓
