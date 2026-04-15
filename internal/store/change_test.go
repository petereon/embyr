package store_test

import (
	"testing"

	"github.com/petereon/firstyr/internal/store"
	"github.com/stretchr/testify/require"
)

func TestDocChange_Fields(t *testing.T) {
	c := store.DocChange{
		Path:       "projects/p/databases/d/documents/col/doc1",
		Collection: "col",
		Parent:     "projects/p/databases/d/documents",
		Kind:       store.DocChangeUpsert,
		Version:    3,
	}
	require.Equal(t, "col", c.Collection)
	require.Equal(t, store.DocChangeUpsert, c.Kind)
	require.EqualValues(t, 1, store.DocChangeDelete)
	require.Equal(t, int64(3), c.Version)
}
