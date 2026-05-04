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

var (
	testGRPCAddr string
	testCancelFn func()
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// 1. Start ministack
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
		os.Exit(m.Run())
	}

	ministackPort, _ := ministackCtr.MappedPort(ctx, "4566/tcp")
	ministackURL := "http://localhost:" + ministackPort.Port()

	os.Setenv("AWS_ENDPOINT_URL", ministackURL)
	os.Setenv("AWS_ACCESS_KEY_ID", "test")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	os.Setenv("AWS_DEFAULT_REGION", "us-east-1")

	// 2. Start GCP Secret Manager emulator
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
		ministackCtr.Terminate(ctx) //nolint:errcheck
		os.Exit(m.Run())
	}

	gcpPort, _ := gcpCtr.MappedPort(ctx, "9090/tcp")
	gcpAddr := "localhost:" + gcpPort.Port()
	os.Setenv("EMBYR_GCP_SECRETMANAGER_EMULATOR_HOST", gcpAddr)

	// 3. Build AWS SDK config
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("aws config: %v", err)
	}

	// 4. Create two RDS Postgres instances via ministack
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

	// 5. Wait for both Postgres instances to accept connections
	registryDSN := waitForRDS(ctx, rdsClient, "embyr-registry", "embyr_registry")
	tenantDSN := waitForRDS(ctx, rdsClient, "embyr-tenant", "embyr_tenant")

	// 6. Set up registry schema
	regDB, err := sql.Open("pgx", registryDSN)
	if err != nil {
		log.Fatalf("open registry db: %v", err)
	}

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

	// 7. Run Embyr migrations on both tenant schemas
	tenantDB, err := sql.Open("pgx", tenantDSN)
	if err != nil {
		log.Fatalf("open tenant db: %v", err)
	}

	runner := migrations.NewRunner("../../migrations/postgres")
	for _, schema := range []string{"embyr_aws_test", "embyr_gcp_test"} {
		if err := runner.Up(ctx, tenantDB, schema); err != nil {
			log.Fatalf("migrate schema %q: %v", schema, err)
		}
	}

	// 8. Seed AWS secret
	smClient := secretsmanager.NewFromConfig(awsCfg)
	awsOut, err := smClient.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String("embyr-aws-tenant-dsn"),
		SecretString: aws.String(tenantDSN),
	})
	if err != nil {
		log.Fatalf("create aws secret: %v", err)
	}
	awsSecretARN := aws.ToString(awsOut.ARN)

	// 9. Seed GCP secret
	gcpConn, err := grpc.NewClient(gcpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial gcp emulator: %v", err)
	}
	gcpSeedClient, err := secretmanager.NewClient(ctx, option.WithGRPCConn(gcpConn))
	if err != nil {
		log.Fatalf("gcp seed client: %v", err)
	}

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
		Parent:  gcpSecret.Name,
		Payload: &secretmanagerpb.SecretPayload{Data: []byte(tenantDSN)},
	}); err != nil {
		log.Fatalf("add gcp secret version: %v", err)
	}
	gcpSecretRef := gcpSecret.Name + "/versions/latest"

	// 10. Insert tenant rows
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

	// 11. Start server.NewMultiTenant in-process
	grpcPort := freePort()
	restPort := freePort()

	cfg := &config.Config{
		Server: config.ServerConfig{
			GRPCPort: grpcPort,
			RESTPort: restPort,
		},
	}

	regClient := registry.NewPostgresClient(regDB, 60*time.Second)
	factory := tenancy.NewAdapterFactory(regClient, 10).WithLogger(zap.NewNop())

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

	// 12. Run tests, then explicit teardown (defer is skipped by os.Exit).
	code := m.Run()
	cancel()
	factory.Close()
	regClient.Close()
	gcpSeedClient.Close()
	gcpConn.Close()
	regDB.Close()
	tenantDB.Close()
	gcpCtr.Terminate(ctx)       //nolint:errcheck
	ministackCtr.Terminate(ctx) //nolint:errcheck
	os.Exit(code)
}

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

func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

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
