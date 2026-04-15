package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/petereon/firstyr/internal/store/sqlite"
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
