package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// #SQ-PANIC (Postgres mirror) — WithTransaction must release the SQL connection
// even when fn panics. A leaked tx in the connection pool eventually exhausts
// it under repeated panics and starves other operations.
func TestPostgresAdapter_WithTransaction_RecoversFromPanic(t *testing.T) {
	a := newTestAdapter(t)

	require.PanicsWithValue(t, "boom", func() {
		_ = a.WithTransaction(context.Background(), func(ctx context.Context) error {
			panic("boom")
		})
	}, "panic must propagate out of WithTransaction")

	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, a.Ping(pingCtx),
		"adapter must remain usable after a panic in WithTransaction (no leaked connection)")
}
