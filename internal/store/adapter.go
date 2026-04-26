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

	// QueryDocuments returns documents matching q. Filters, ordering, and cursors
	// are applied; results are paginated using q.PageSize and q.PageToken.
	QueryDocuments(ctx context.Context, q *Query) (*ListPage, error)

	// WithTransaction executes fn inside a SQL transaction.
	// If fn returns an error the transaction is rolled back; otherwise committed.
	WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error

	// BeginTransaction records a new Firestore transaction and returns its ID.
	BeginTransaction(ctx context.Context, readOnly bool) (txID string, err error)

	// GetDocumentForTransaction fetches a document and records the read in the
	// transaction's read set for OCC validation at commit.
	GetDocumentForTransaction(ctx context.Context, txID, path string) (*Document, error)

	// CommitTransaction verifies OCC, applies ops atomically, deletes transaction record.
	CommitTransaction(ctx context.Context, txID string, ops []WriteOp) (*CommitResult, error)

	// RollbackTransaction deletes the transaction record without applying writes.
	RollbackTransaction(ctx context.Context, txID string) error

	// SweepExpiredTransactions deletes transaction records whose expires_at is in the past.
	SweepExpiredTransactions(ctx context.Context) (deleted int, err error)

	// RunAggregationQuery executes an aggregation query and returns one result
	// value per Aggregation in q.Aggregations, keyed by Aggregation.Alias.
	RunAggregationQuery(ctx context.Context, q *AggregationQuery) (map[string]AggregateValue, error)

	// ListCollectionIds returns the distinct collection IDs of immediate child
	// collections of the document at parent. pageSize=0 uses a server default.
	// pageToken="" starts from the first page.
	ListCollectionIds(ctx context.Context, parent string, pageSize int32, pageToken string) (collectionIDs []string, nextPageToken string, err error)

	// Subscribe returns a channel that receives DocChange events for every
	// committed write to this database. The subscription lives until ctx is
	// cancelled or the returned cancel function is called, whichever comes first.
	// Multiple concurrent subscribers are supported; each receives all changes.
	Subscribe(ctx context.Context) (<-chan DocChange, func())
}
