package store

import "context"

// StorageAdapter is the single interface both the SQLite and PostgreSQL backends implement.
// Methods are added incrementally across plans:
//   - Plan 1: Ping, Close, Migrate
//   - Plan 2: document CRUD (CreateDocument, GetDocument, UpdateDocument,
//             DeleteDocument, ListDocuments)
//   - Plan 3: RunQuery, BeginTransaction, CommitTransaction, RollbackTransaction, BatchWrite
//   - Plan 4: WatchCollection, UnwatchCollection
type StorageAdapter interface {
	// Ping checks that the database is reachable. Used by /readyz.
	Ping(ctx context.Context) error

	// Migrate runs all pending schema migrations.
	Migrate(ctx context.Context) error

	// Close releases all database connections.
	Close() error

	// CreateDocument inserts doc. Returns codes.AlreadyExists if doc.Path is taken.
	CreateDocument(ctx context.Context, doc *Document) (*Document, error)

	// GetDocument fetches the document at path.
	// Returns a gRPC codes.NotFound status error if the document is absent.
	GetDocument(ctx context.Context, path string) (*Document, error)

	// UpdateDocument writes doc according to mode:
	//   WriteModeUpsert     — create or update (no existence requirement)
	//   WriteModeUpdate     — must exist; returns codes.NotFound if absent
	//   WriteModeInsertOnly — must not exist; returns codes.AlreadyExists if present
	// The doc's Data field must contain the complete final state after any
	// field-mask merging has been applied by the caller.
	UpdateDocument(ctx context.Context, doc *Document, mode WriteMode) (*Document, error)

	// DeleteDocument removes the document at path.
	// If mustExist is true and the document is absent, returns codes.NotFound.
	// If mustExist is false, deletion is idempotent (no error if absent).
	DeleteDocument(ctx context.Context, path string, mustExist bool) error

	// ListDocuments returns documents directly under parent whose collection
	// matches collectionID. If collectionID is empty, all collections under
	// parent are returned.
	// pageSize=0 uses a server default of 100.
	// pageToken="" starts from the first page.
	ListDocuments(ctx context.Context, parent, collectionID string, pageSize int32, pageToken string) (*ListPage, error)
}
