package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/petereon/firstyr/internal/store/sqlite"
	"github.com/stretchr/testify/require"
)

// #SQ-PANIC — WithTransaction must release the SQL connection even when fn
// panics. With MaxOpenConns=1, a leaked tx after a panic deadlocks the entire
// adapter; the next operation hangs forever.
func TestSQLite_WithTransaction_RecoversFromPanic(t *testing.T) {
	dir := t.TempDir()
	a, err := sqlite.New(filepath.Join(dir, "panic.db"), "../../../migrations/sqlite")
	require.NoError(t, err)
	t.Cleanup(func() { a.Close() })
	require.NoError(t, a.Migrate(context.Background()))

	// Trigger a panic inside fn. The recovered panic must propagate, and the
	// underlying SQL transaction must be rolled back so the connection is
	// returned to the pool.
	require.PanicsWithValue(t, "boom", func() {
		_ = a.WithTransaction(context.Background(), func(ctx context.Context) error {
			panic("boom")
		})
	}, "panic must propagate out of WithTransaction")

	// If the connection is leaked, this Ping deadlocks (MaxOpenConns=1).
	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, a.Ping(pingCtx),
		"adapter must remain usable after a panic in WithTransaction (no leaked connection)")
}
