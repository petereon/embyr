package sqlite_test

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/petereon/embyr/internal/store/sqlite"
	"github.com/stretchr/testify/require"
)

// #SUB-CTX-LEAK — When the caller invokes cancel() returned by Subscribe
// while the parent ctx is still live, the ctx-watcher goroutine must exit
// promptly instead of parking on <-ctx.Done() until the parent eventually
// cancels.
func TestSQLite_Subscribe_CancelReleasesGoroutineImmediately(t *testing.T) {
	dir := t.TempDir()
	a, err := sqlite.New(filepath.Join(dir, "sub.db"), "../../../migrations/sqlite")
	require.NoError(t, err)
	t.Cleanup(func() { a.Close() })
	require.NoError(t, a.Migrate(context.Background()))

	parentCtx, parentCancel := context.WithCancel(context.Background())
	t.Cleanup(parentCancel)

	runtime.GC()
	baseline := runtime.NumGoroutine()

	const cycles = 50
	for i := 0; i < cycles; i++ {
		_, cancel := a.Subscribe(parentCtx)
		cancel()
	}

	// If each cancel leaks the ctx-watcher goroutine, NumGoroutine grows
	// linearly with cycles. With the fix, all goroutines should release.
	require.Eventually(t, func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= baseline+5
	}, 2*time.Second, 20*time.Millisecond,
		"Subscribe ctx-watcher goroutines must release on explicit cancel (baseline=%d, current=%d, cycles=%d)",
		baseline, runtime.NumGoroutine(), cycles)
}
