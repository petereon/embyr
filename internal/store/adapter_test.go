package store_test

import (
	"testing"
	"github.com/petereon/firstyr/internal/store"
)

// TestStorageAdapterInterface is a compile-time check that the interface is consistent.
func TestStorageAdapterInterface(t *testing.T) {
	var _ store.StorageAdapter = (store.StorageAdapter)(nil)
}
