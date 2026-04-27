package store_test

import (
	"testing"

	"github.com/petereon/embyr/internal/store"
	"github.com/stretchr/testify/require"
)

func TestFilterValue_ZeroValues(t *testing.T) {
	fv := store.FilterValue{Kind: store.FilterValueNull}
	require.Equal(t, store.FilterValueNull, fv.Kind)

	fv2 := store.FilterValue{Kind: store.FilterValueInt, IntVal: 42}
	require.Equal(t, int64(42), fv2.IntVal)
}
