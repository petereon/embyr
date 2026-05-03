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

	require.NoError(t, r.Up(ctx, db, schema))

	var exists bool
	err := db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables
		 WHERE table_schema=$1 AND table_name='documents')`, schema).Scan(&exists)
	require.NoError(t, err)
	assert.True(t, exists, "documents table should exist after Up")

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
	require.NoError(t, r.Up(ctx, db, schema))
}

func TestRunner_InvalidSchemaName(t *testing.T) {
	r := migrations.NewRunner("../../migrations/postgres")
	err := r.Up(context.Background(), nil, "bad schema!")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid schema name")
}
