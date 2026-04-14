package store

import "context"

// StorageAdapter is the single interface both the SQLite and PostgreSQL backends implement.
// Methods are added incrementally across plans:
//   - Plan 1: Ping, Close, Migrate
//   - Plan 2: document CRUD (GetDocument, SetDocument, UpdateDocument, DeleteDocument, ListDocuments)
//   - Plan 3: RunQuery, BeginTransaction, CommitTransaction, RollbackTransaction, BatchWrite
//   - Plan 4: WatchCollection, UnwatchCollection
type StorageAdapter interface {
	// Ping checks that the database is reachable. Used by /readyz.
	Ping(ctx context.Context) error

	// Migrate runs all pending schema migrations.
	Migrate(ctx context.Context) error

	// Close releases all database connections.
	Close() error
}
