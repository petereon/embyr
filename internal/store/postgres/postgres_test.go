package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/petereon/firstyr/internal/store/postgres"
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
