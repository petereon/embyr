package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/petereon/embyr/internal/store/sqlite"
	"github.com/stretchr/testify/require"
)

// #CFG1 (TTL half) — Adapter.SetTransactionTTL must change the expires_at
// written by BeginTransaction. With TTL=2s, a freshly begun transaction must
// have expires_at within ~2s of now (not 60s, the previous hardcode).
func TestSQLite_BeginTransaction_HonorsConfiguredTTL(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "ttl.db")
	a, err := sqlite.New(dbPath, "../../../migrations/sqlite")
	require.NoError(t, err)
	t.Cleanup(func() { a.Close() })
	require.NoError(t, a.Migrate(context.Background()))

	a.SetTransactionTTL(2 * time.Second)

	before := time.Now().UTC()
	id, err := a.BeginTransaction(context.Background(), false)
	require.NoError(t, err)

	// Inspect expires_at directly via a fresh connection to avoid touching
	// the adapter's internal state.
	raw, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer raw.Close()
	var expiresStr string
	require.NoError(t, raw.QueryRow(
		`SELECT expires_at FROM transactions WHERE id=?`, id).Scan(&expiresStr))

	expires, err := time.Parse(time.RFC3339Nano, expiresStr)
	require.NoError(t, err)

	delta := expires.Sub(before)
	require.GreaterOrEqual(t, delta, 1500*time.Millisecond,
		"expires_at must be at least ~TTL into the future")
	require.LessOrEqual(t, delta, 5*time.Second,
		"expires_at must not be using the 60s hardcoded default; got %s", delta)
}
