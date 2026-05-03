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

func TestPostgresClient_Get_SuspendedTenant(t *testing.T) {
	db := openTestDB(t)
	suspended := registry.Tenant{
		ID: "test-suspended", ProjectID: "susp", DatabaseID: "db",
		SchemaName: "embyr_db", CredentialType: registry.CredentialGCPSecret,
		CredentialRef: "projects/x/secrets/y/versions/1", AuthMode: "none",
		AuthConfig: json.RawMessage(`{}`), Status: registry.TenantStatusSuspended,
	}
	seedTenant(t, db, suspended)

	client := registry.NewPostgresClient(db, 60*time.Second)
	defer client.Close()

	_, err := client.Get(context.Background(), "susp", "db")
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
