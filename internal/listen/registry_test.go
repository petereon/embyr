package listen_test

import (
	"testing"
	"time"

	"github.com/petereon/embyr/internal/listen"
	"github.com/petereon/embyr/internal/store"
	"github.com/stretchr/testify/require"
)

func TestRegistry_DispatchAndReceive(t *testing.T) {
	r := listen.NewRegistry()

	ch, unsub := r.Subscribe()
	defer unsub()

	change := store.DocChange{
		Path:       "projects/p/databases/d/documents/col/doc1",
		Collection: "col",
		Parent:     "projects/p/databases/d/documents",
		Kind:       store.DocChangeUpsert,
		Version:    1,
		Data:       `{"fields":{}}`,
	}
	r.Dispatch(change)

	select {
	case got := <-ch:
		require.Equal(t, change.Path, got.Path)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for change")
	}
}

func TestRegistry_MultipleSubscribers(t *testing.T) {
	r := listen.NewRegistry()

	ch1, unsub1 := r.Subscribe()
	ch2, unsub2 := r.Subscribe()
	defer unsub1()
	defer unsub2()

	r.Dispatch(store.DocChange{Path: "p/d", Kind: store.DocChangeUpsert})

	timeout := time.After(time.Second)
	for i := 0; i < 2; i++ {
		select {
		case <-ch1: // ok
		case <-ch2: // ok
		case <-timeout:
			t.Fatalf("timeout after %d of 2 receives", i)
		}
	}
}

func TestRegistry_Unsubscribe(t *testing.T) {
	r := listen.NewRegistry()
	_, unsub := r.Subscribe()
	unsub()
	// Should not block or panic when dispatching after unsub.
	r.Dispatch(store.DocChange{Path: "p"})
}
