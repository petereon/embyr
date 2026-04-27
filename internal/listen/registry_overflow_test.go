package listen_test

import (
	"testing"

	"github.com/petereon/embyr/internal/listen"
	"github.com/petereon/embyr/internal/store"
	"github.com/stretchr/testify/require"
)

// #SUB-DROP — Registry must expose when changes were dropped to a slow
// subscriber so the Listen handler can recover via a RESET to the client.
//
// We Dispatch many more events than the per-subscriber buffer can hold, then
// inspect the subscription's Overflowed() flag.
func TestRegistry_OverflowSignal(t *testing.T) {
	r := listen.NewRegistry()

	sub := r.SubscribeWithSignal()
	t.Cleanup(sub.Cancel)

	// The subscriber never drains — buffer fills, subsequent dispatches must
	// be dropped and the overflow signal must be set.
	for i := 0; i < 1024; i++ {
		r.Dispatch(store.DocChange{
			Path: "projects/p/databases/d/documents/col/doc",
			Kind: store.DocChangeUpsert,
		})
	}

	require.True(t, sub.Overflowed(),
		"subscriber overflow flag must be set after dispatches exceed buffer capacity")
}

// Dispatches that fit in the channel buffer must NOT trigger the overflow
// signal — the flag is only meaningful when changes were actually dropped.
func TestRegistry_NoOverflowWhenWithinCapacity(t *testing.T) {
	r := listen.NewRegistry()

	sub := r.SubscribeWithSignal()
	t.Cleanup(sub.Cancel)

	// Buffer is 64; dispatching fewer than that without draining must not
	// trigger overflow.
	for i := 0; i < 32; i++ {
		r.Dispatch(store.DocChange{
			Path: "projects/p/databases/d/documents/col/doc",
			Kind: store.DocChangeUpsert,
		})
	}

	require.False(t, sub.Overflowed(),
		"overflow flag must remain unset when buffer can hold all dispatches")
}
