package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/golang-migrate/migrate/v4"
	migratesqlite "github.com/golang-migrate/migrate/v4/database/sqlite"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "modernc.org/sqlite"

	"github.com/petereon/firstyr/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Compile-time check that Adapter implements store.StorageAdapter.
var _ store.StorageAdapter = (*Adapter)(nil)

// sqlExecer is the common interface shared by *sql.DB and *sql.Tx, allowing
// CRUD methods to work transparently inside or outside a transaction.
type sqlExecer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// ctxTxKey is the context key used to pass a *sql.Tx through context.
type ctxTxKey struct{}

// txFromCtx extracts a *sql.Tx from the context, if present.
func txFromCtx(ctx context.Context) sqlExecer {
	if tx, ok := ctx.Value(ctxTxKey{}).(*sql.Tx); ok && tx != nil {
		return tx
	}
	return nil
}

// execer returns the transaction from ctx if present, otherwise a.db.
// This is the concurrency-safe alternative to mutating a.db on the shared struct.
func (a *Adapter) execer(ctx context.Context) sqlExecer {
	if tx := txFromCtx(ctx); tx != nil {
		return tx
	}
	return a.db
}

// validFieldPath matches safe Firestore field path identifiers.
var validFieldPath = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)

// validateFieldPath returns an InvalidArgument error if path contains
// characters that could be used for SQL injection.
func validateFieldPath(path string) error {
	if !validFieldPath.MatchString(path) {
		return status.Errorf(codes.InvalidArgument, "invalid field path: %q", path)
	}
	return nil
}

// Adapter implements store.StorageAdapter for SQLite.
type Adapter struct {
	rawDB          *sql.DB   // for Ping, Close, BeginTx, migrations
	db             sqlExecer // for CRUD (never mutated after construction)
	migrationsPath string
	// subscriber registry for real-time change notifications
	subsMu  sync.RWMutex
	subs    map[uint64]chan store.DocChange
	subNext uint64
}

// New opens (or creates) a SQLite database at path and returns a ready Adapter.
func New(path, migrationsPath string) (*Adapter, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open %s: %w", path, err)
	}
	// WAL mode enables concurrent reads with writes and is required for listener hooks.
	// SQLite PRAGMA returns the resulting mode — must verify it actually switched to wal,
	// since a locked database may silently stay in the previous journal mode.
	var mode string
	if err := db.QueryRow("PRAGMA journal_mode=WAL;").Scan(&mode); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: enable WAL: %w", err)
	}
	if mode != "wal" {
		db.Close()
		return nil, fmt.Errorf("sqlite: enable WAL: got mode %q, want \"wal\"", mode)
	}
	return &Adapter{rawDB: db, db: db, migrationsPath: migrationsPath}, nil
}

// Ping verifies the database connection is alive.
func (a *Adapter) Ping(ctx context.Context) error {
	return a.rawDB.PingContext(ctx)
}

// Migrate applies all pending up-migrations from migrationsPath.
func (a *Adapter) Migrate(ctx context.Context) error {
	driver, err := migratesqlite.WithInstance(a.rawDB, &migratesqlite.Config{})
	if err != nil {
		return fmt.Errorf("sqlite: migrate driver: %w", err)
	}
	m, err := migrate.NewWithDatabaseInstance(
		"file://"+a.migrationsPath,
		"sqlite", driver,
	)
	if err != nil {
		return fmt.Errorf("sqlite: migrate init: %w", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("sqlite: migrate up: %w", err)
	}
	return nil
}

// Close releases the database connection pool.
func (a *Adapter) Close() error {
	return a.rawDB.Close()
}

// notifySubscribers sends c to all current subscribers (non-blocking).
func (a *Adapter) notifySubscribers(c store.DocChange) {
	a.subsMu.RLock()
	defer a.subsMu.RUnlock()
	for _, ch := range a.subs {
		select {
		case ch <- c:
		default:
		}
	}
}

// Subscribe returns a channel that receives DocChange events for every
// committed write. The returned cancel function unregisters the subscriber
// and closes the channel.
func (a *Adapter) Subscribe() (<-chan store.DocChange, func()) {
	ch := make(chan store.DocChange, 64)
	a.subsMu.Lock()
	id := a.subNext
	a.subNext++
	if a.subs == nil {
		a.subs = make(map[uint64]chan store.DocChange)
	}
	a.subs[id] = ch
	a.subsMu.Unlock()
	return ch, func() {
		a.subsMu.Lock()
		delete(a.subs, id)
		a.subsMu.Unlock()
		close(ch)
		for range ch {
		}
	}
}

// ─── Document CRUD ────────────────────────────────────────────────────────────

const (
	sqlInsertDoc = `
        INSERT INTO documents (path, collection, parent, data, created_at, updated_at, version)
        VALUES (?, ?, ?, ?, ?, ?, 1)
        RETURNING path, data, created_at, updated_at, version`

	sqlGetDoc = `
        SELECT path, data, created_at, updated_at, version
        FROM documents WHERE path = ?`

	sqlUpsertDoc = `
        INSERT INTO documents (path, collection, parent, data, created_at, updated_at, version)
        VALUES (?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(path) DO UPDATE SET
            data       = excluded.data,
            updated_at = excluded.updated_at,
            version    = documents.version + 1
        RETURNING path, data, created_at, updated_at, version`

	sqlUpdateDoc = `
        UPDATE documents SET data=?, updated_at=?, version=version+1
        WHERE path=?
        RETURNING path, data, created_at, updated_at, version`

	sqlDeleteDoc = `DELETE FROM documents WHERE path = ?`
)

// sqliteParseCollection derives the collection name and parent path from a
// Firestore document path. This duplicates codec.ParsePath to avoid an import
// cycle (codec imports store; sqlite is a sub-package of store).
func sqliteParseCollection(path string) (collection, parent string) {
	parts := strings.Split(path, "/")
	docsIdx := -1
	for i, p := range parts {
		if p == "documents" {
			docsIdx = i
			break
		}
	}
	if docsIdx < 0 {
		return "", ""
	}
	relative := parts[docsIdx+1:]
	if len(relative) < 2 || len(relative)%2 != 0 {
		return "", ""
	}
	collection = relative[len(relative)-2]
	parentParts := make([]string, 0, docsIdx+1+len(relative)-2)
	parentParts = append(parentParts, parts[:docsIdx+1]...)
	parentParts = append(parentParts, relative[:len(relative)-2]...)
	parent = strings.Join(parentParts, "/")
	return collection, parent
}

func scanDoc(row interface {
	Scan(dest ...any) error
}) (*store.Document, error) {
	var d store.Document
	var createdStr, updatedStr string
	if err := row.Scan(&d.Path, &d.Data, &createdStr, &updatedStr, &d.Version); err != nil {
		return nil, err
	}
	var err error
	if d.CreatedAt, err = time.Parse(time.RFC3339Nano, createdStr); err != nil {
		return nil, fmt.Errorf("sqlite: parse created_at %q: %w", createdStr, err)
	}
	if d.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedStr); err != nil {
		return nil, fmt.Errorf("sqlite: parse updated_at %q: %w", updatedStr, err)
	}
	return &d, nil
}

// CreateDocument inserts a new document. Returns codes.AlreadyExists if path taken.
func (a *Adapter) CreateDocument(ctx context.Context, doc *store.Document) (*store.Document, error) {
	now := time.Now().UTC()
	if doc.CreatedAt.IsZero() {
		doc.CreatedAt = now
	}
	if doc.UpdatedAt.IsZero() {
		doc.UpdatedAt = now
	}
	collection, parent := sqliteParseCollection(doc.Path)
	if collection == "" {
		return nil, status.Errorf(codes.InvalidArgument, "sqlite: invalid document path: %s", doc.Path)
	}
	updatedAt := doc.UpdatedAt.UTC().Format(time.RFC3339Nano)
	createdAt := doc.CreatedAt.UTC().Format(time.RFC3339Nano)

	row := a.execer(ctx).QueryRowContext(ctx, sqlInsertDoc,
		doc.Path, collection, parent, doc.Data, createdAt, updatedAt)
	d, err := scanDoc(row)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return nil, status.Errorf(codes.AlreadyExists, "sqlite: document already exists: %s", doc.Path)
		}
		return nil, fmt.Errorf("sqlite: create document: %w", err)
	}
	a.notifySubscribers(store.DocChange{
		Path:       d.Path,
		Collection: collection,
		Parent:     parent,
		Kind:       store.DocChangeUpsert,
		Version:    d.Version,
		Data:       d.Data,
	})
	return d, nil
}

// GetDocument fetches a document by path. Returns codes.NotFound if absent.
func (a *Adapter) GetDocument(ctx context.Context, path string) (*store.Document, error) {
	row := a.execer(ctx).QueryRowContext(ctx, sqlGetDoc, path)
	d, err := scanDoc(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "sqlite: document not found: %s", path)
		}
		return nil, fmt.Errorf("sqlite: get document: %w", err)
	}
	return d, nil
}

// UpdateDocument writes doc according to mode.
func (a *Adapter) UpdateDocument(ctx context.Context, doc *store.Document, mode store.WriteMode) (*store.Document, error) {
	if doc.UpdatedAt.IsZero() {
		doc.UpdatedAt = time.Now().UTC()
	}
	if doc.CreatedAt.IsZero() {
		doc.CreatedAt = doc.UpdatedAt
	}
	now := doc.UpdatedAt.UTC().Format(time.RFC3339Nano)

	switch mode {
	case store.WriteModeUpsert:
		collection, parent := sqliteParseCollection(doc.Path)
		if collection == "" {
			return nil, status.Errorf(codes.InvalidArgument, "sqlite: invalid document path: %s", doc.Path)
		}
		createdAt := doc.CreatedAt.UTC().Format(time.RFC3339Nano)
		row := a.execer(ctx).QueryRowContext(ctx, sqlUpsertDoc,
			doc.Path, collection, parent, doc.Data, createdAt, now)
		d, err := scanDoc(row)
		if err != nil {
			return nil, fmt.Errorf("sqlite: upsert document: %w", err)
		}
		a.notifySubscribers(store.DocChange{
			Path:       d.Path,
			Collection: collection,
			Parent:     parent,
			Kind:       store.DocChangeUpsert,
			Version:    d.Version,
			Data:       d.Data,
		})
		return d, nil

	case store.WriteModeUpdate:
		row := a.execer(ctx).QueryRowContext(ctx, sqlUpdateDoc, doc.Data, now, doc.Path)
		d, err := scanDoc(row)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, status.Errorf(codes.NotFound, "sqlite: document not found: %s", doc.Path)
			}
			return nil, fmt.Errorf("sqlite: update document: %w", err)
		}
		collection, parent := sqliteParseCollection(d.Path)
		a.notifySubscribers(store.DocChange{
			Path:       d.Path,
			Collection: collection,
			Parent:     parent,
			Kind:       store.DocChangeUpsert,
			Version:    d.Version,
			Data:       d.Data,
		})
		return d, nil

	case store.WriteModeInsertOnly:
		collection, parent := sqliteParseCollection(doc.Path)
		if collection == "" {
			return nil, status.Errorf(codes.InvalidArgument, "sqlite: invalid document path: %s", doc.Path)
		}
		createdAt := doc.CreatedAt.UTC().Format(time.RFC3339Nano)
		row := a.execer(ctx).QueryRowContext(ctx, sqlInsertDoc,
			doc.Path, collection, parent, doc.Data, createdAt, now)
		d, err := scanDoc(row)
		if err != nil {
			if strings.Contains(err.Error(), "UNIQUE constraint failed") {
				return nil, status.Errorf(codes.AlreadyExists, "sqlite: document already exists: %s", doc.Path)
			}
			return nil, fmt.Errorf("sqlite: insert-only document: %w", err)
		}
		a.notifySubscribers(store.DocChange{
			Path:       d.Path,
			Collection: collection,
			Parent:     parent,
			Kind:       store.DocChangeUpsert,
			Version:    d.Version,
			Data:       d.Data,
		})
		return d, nil

	default:
		return nil, fmt.Errorf("sqlite: unknown write mode %d", mode)
	}
}

// DeleteDocument removes a document. Returns codes.NotFound if mustExist and absent.
func (a *Adapter) DeleteDocument(ctx context.Context, path string, mustExist bool) error {
	result, err := a.execer(ctx).ExecContext(ctx, sqlDeleteDoc, path)
	if err != nil {
		return fmt.Errorf("sqlite: delete document: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: delete rows affected: %w", err)
	}
	if mustExist && n == 0 {
		return status.Errorf(codes.NotFound, "sqlite: document not found: %s", path)
	}
	if n > 0 {
		collection, parent := sqliteParseCollection(path)
		a.notifySubscribers(store.DocChange{
			Path:       path,
			Collection: collection,
			Parent:     parent,
			Kind:       store.DocChangeDelete,
		})
	}
	return nil
}

// ListDocuments returns documents in a collection, paginated by offset.
func (a *Adapter) ListDocuments(ctx context.Context, parent, collectionID string, pageSize int32, pageToken string) (*store.ListPage, error) {
	if pageSize <= 0 {
		pageSize = 100
	}
	offset := store.DecodePageToken(pageToken)
	fetchSize := int(pageSize) + 1 // look-ahead: one extra to detect next page

	var (
		rows *sql.Rows
		err  error
	)
	if collectionID == "" {
		rows, err = a.execer(ctx).QueryContext(ctx,
			`SELECT path, data, created_at, updated_at, version
             FROM documents WHERE parent = ?
             ORDER BY path ASC LIMIT ? OFFSET ?`,
			parent, fetchSize, offset)
	} else {
		rows, err = a.execer(ctx).QueryContext(ctx,
			`SELECT path, data, created_at, updated_at, version
             FROM documents WHERE collection = ? AND parent = ?
             ORDER BY path ASC LIMIT ? OFFSET ?`,
			collectionID, parent, fetchSize, offset)
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite: list documents: %w", err)
	}
	defer rows.Close()

	docs := make([]*store.Document, 0, pageSize)
	for rows.Next() {
		d, err := scanDoc(rows)
		if err != nil {
			return nil, fmt.Errorf("sqlite: scan document row: %w", err)
		}
		docs = append(docs, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: list documents rows: %w", err)
	}

	var nextToken string
	if len(docs) > int(pageSize) {
		docs = docs[:pageSize]
		nextToken = store.EncodePageToken(offset + int(pageSize))
	}
	return &store.ListPage{Documents: docs, NextPageToken: nextToken}, nil
}

// ─── Query helpers ────────────────────────────────────────────────────────────

// sqliteFieldBase builds the json_extract path prefix for a (possibly nested)
// Firestore field path, e.g. "foo.bar" → "$.fields.foo.mapValue.fields.bar".
func sqliteFieldBase(fieldPath string) string {
	parts := strings.Split(fieldPath, ".")
	var sb strings.Builder
	sb.WriteString("$.fields.")
	for i, p := range parts {
		sb.WriteString(p)
		if i < len(parts)-1 {
			sb.WriteString(".mapValue.fields.")
		}
	}
	return sb.String()
}

// sqliteTypedPath appends the type-specific leaf key to a base path.
func sqliteTypedPath(base string, kind store.FilterValueKind) string {
	switch kind {
	case store.FilterValueString:
		return base + ".stringValue"
	case store.FilterValueInt:
		return base + ".integerValue"
	case store.FilterValueDouble:
		return base + ".doubleValue"
	case store.FilterValueBool:
		return base + ".booleanValue"
	case store.FilterValueTime:
		return base + ".timestampValue"
	default:
		return base
	}
}

// sqliteScalarExpr returns a SQL expression and bound argument for a scalar FilterValue.
func sqliteScalarExpr(fieldPath string, fv store.FilterValue) (expr string, arg interface{}) {
	base := sqliteFieldBase(fieldPath)
	path := sqliteTypedPath(base, fv.Kind)
	switch fv.Kind {
	case store.FilterValueInt:
		return fmt.Sprintf("CAST(json_extract(data,'%s') AS REAL)", path), float64(fv.IntVal)
	case store.FilterValueDouble:
		return fmt.Sprintf("CAST(json_extract(data,'%s') AS REAL)", path), fv.DoubleVal
	case store.FilterValueBool:
		b := 0
		if fv.BoolVal {
			b = 1
		}
		return fmt.Sprintf("json_extract(data,'%s')", path), b
	case store.FilterValueTime:
		return fmt.Sprintf("json_extract(data,'%s')", path), fv.TimeVal.UTC().Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("json_extract(data,'%s')", path), fv.StrVal
	}
}

// sqliteFilterClause builds a SQL WHERE clause fragment for a single FieldFilter.
func sqliteFilterClause(f store.FieldFilter, args *[]interface{}) (string, error) {
	if err := validateFieldPath(f.Field); err != nil {
		return "", err
	}
	switch f.Op {
	case store.FilterOpEqual, store.FilterOpNotEqual,
		store.FilterOpLessThan, store.FilterOpLessThanOrEqual,
		store.FilterOpGreaterThan, store.FilterOpGreaterThanOrEqual:
		expr, arg := sqliteScalarExpr(f.Field, f.Value)
		*args = append(*args, arg)
		opMap := map[store.FilterOp]string{
			store.FilterOpEqual:              "=",
			store.FilterOpNotEqual:           "!=",
			store.FilterOpLessThan:           "<",
			store.FilterOpLessThanOrEqual:    "<=",
			store.FilterOpGreaterThan:        ">",
			store.FilterOpGreaterThanOrEqual: ">=",
		}
		return fmt.Sprintf("%s %s ?", expr, opMap[f.Op]), nil

	case store.FilterOpArrayContains:
		base := sqliteFieldBase(f.Field)
		arrayPath := base + ".arrayValue.values"
		_, arg := sqliteScalarExpr(f.Field, f.Value)
		typeKey := ""
		switch f.Value.Kind {
		case store.FilterValueString:
			typeKey = "$.stringValue"
		case store.FilterValueInt:
			typeKey = "$.integerValue"
		case store.FilterValueDouble:
			typeKey = "$.doubleValue"
		case store.FilterValueBool:
			typeKey = "$.booleanValue"
		}
		*args = append(*args, arg)
		return fmt.Sprintf(
			"EXISTS (SELECT 1 FROM json_each(json_extract(data,'%s')) AS e WHERE json_extract(e.value,'%s') = ?)",
			arrayPath, typeKey), nil

	case store.FilterOpIn, store.FilterOpNotIn:
		base := sqliteFieldBase(f.Field)
		kind := store.FilterValueNull
		if len(f.Value.ArrayVals) > 0 {
			kind = f.Value.ArrayVals[0].Kind
		}
		path := sqliteTypedPath(base, kind)
		phs := make([]string, len(f.Value.ArrayVals))
		for i, v := range f.Value.ArrayVals {
			phs[i] = "?"
			_, arg := sqliteScalarExpr(f.Field, v)
			*args = append(*args, arg)
		}
		op := "IN"
		if f.Op == store.FilterOpNotIn {
			op = "NOT IN"
		}
		return fmt.Sprintf("json_extract(data,'%s') %s (%s)", path, op, strings.Join(phs, ",")), nil

	default:
		return "", status.Errorf(codes.Unimplemented, "filter op %v not supported in sqlite", f.Op)
	}
}

// sqliteOrderExpr builds an ORDER BY expression for a single OrderBy clause.
func sqliteOrderExpr(o store.OrderBy) (string, error) {
	if o.Direction != store.DirectionAsc && o.Direction != store.DirectionDesc {
		return "", status.Errorf(codes.InvalidArgument, "invalid order direction: %q", o.Direction)
	}
	if err := validateFieldPath(o.Field); err != nil {
		return "", err
	}
	base := sqliteFieldBase(o.Field)
	return fmt.Sprintf("json_extract(data,'%s') %s", base, string(o.Direction)), nil
}

// QueryDocuments executes a structured query and returns matching documents.
func (a *Adapter) QueryDocuments(ctx context.Context, q *store.Query) (*store.ListPage, error) {
	pageSize := q.PageSize
	if pageSize <= 0 {
		pageSize = 300
	}
	offset := store.DecodePageToken(q.PageToken)
	fetchSize := int(pageSize) + 1

	var sb strings.Builder
	args := make([]interface{}, 0, 8)

	sb.WriteString(`SELECT path, data, created_at, updated_at, version FROM documents WHERE parent = ?`)
	args = append(args, q.Parent)

	if q.CollectionID != "" {
		sb.WriteString(` AND collection = ?`)
		args = append(args, q.CollectionID)
	}

	if q.Filter != nil {
		for _, f := range q.Filter.Filters {
			clause, err := sqliteFilterClause(f, &args)
			if err != nil {
				return nil, err
			}
			sb.WriteString(" AND ")
			sb.WriteString(clause)
		}
	}

	if len(q.OrderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		for i, o := range q.OrderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			expr, err := sqliteOrderExpr(o)
			if err != nil {
				return nil, err
			}
			sb.WriteString(expr)
		}
	} else {
		sb.WriteString(" ORDER BY path ASC")
	}

	sb.WriteString(fmt.Sprintf(" LIMIT %d OFFSET %d", fetchSize, offset))

	rows, err := a.execer(ctx).QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: query documents: %w", err)
	}
	defer rows.Close()

	docs := make([]*store.Document, 0, pageSize)
	for rows.Next() {
		d, err := scanDoc(rows)
		if err != nil {
			return nil, fmt.Errorf("sqlite: scan query row: %w", err)
		}
		docs = append(docs, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: query rows: %w", err)
	}

	var nextToken string
	if len(docs) > int(pageSize) {
		docs = docs[:pageSize]
		nextToken = store.EncodePageToken(offset + int(pageSize))
	}
	return &store.ListPage{Documents: docs, NextPageToken: nextToken}, nil
}

// ─── Transaction support ──────────────────────────────────────────────────────

// WithTransaction executes fn inside a SQL transaction. If fn returns an error
// the transaction is rolled back; otherwise it is committed.
// The transaction is passed via context so that concurrent calls on the same
// Adapter do not race on a shared field — each goroutine gets its own txCtx.
func (a *Adapter) WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := a.rawDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: begin tx: %w", err)
	}
	txCtx := context.WithValue(ctx, ctxTxKey{}, tx)
	if err := fn(txCtx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// newTxID generates a random 128-bit hex transaction identifier.
func newTxID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// BeginTransaction records a new Firestore transaction and returns its ID.
// readOnly is recorded for future enforcement; writes are always permitted in plan 3.
func (a *Adapter) BeginTransaction(ctx context.Context, readOnly bool) (string, error) {
	_ = readOnly // reserved for future read-only enforcement
	id := newTxID()
	t := time.Now().UTC()
	now := t.Format(time.RFC3339Nano)
	expires := t.Add(60 * time.Second).Format(time.RFC3339Nano)
	_, err := a.execer(ctx).ExecContext(ctx,
		`INSERT INTO transactions (id, started_at, reads, expires_at) VALUES (?, ?, '{}', ?)`,
		id, now, expires)
	if err != nil {
		return "", fmt.Errorf("sqlite: begin transaction: %w", err)
	}
	return id, nil
}

// GetDocumentForTransaction fetches a document and records the read in the
// transaction's read set for OCC validation at commit.
func (a *Adapter) GetDocumentForTransaction(ctx context.Context, txID, path string) (*store.Document, error) {
	doc, err := a.GetDocument(ctx, path)
	if err != nil {
		return nil, err
	}
	_, err = a.execer(ctx).ExecContext(ctx,
		`UPDATE transactions SET reads = json_set(reads, '$.' || ?, ?) WHERE id = ?`,
		path, doc.Version, txID)
	if err != nil {
		return nil, fmt.Errorf("sqlite: record transaction read: %w", err)
	}
	return doc, nil
}

// CommitTransaction verifies OCC, applies ops atomically, and deletes the
// transaction record.
func (a *Adapter) CommitTransaction(ctx context.Context, txID string, ops []store.WriteOp) (*store.CommitResult, error) {
	var readsJSON string
	err := a.rawDB.QueryRowContext(ctx,
		`SELECT reads FROM transactions WHERE id = ?`, txID).Scan(&readsJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "sqlite: transaction not found: %s", txID)
		}
		return nil, fmt.Errorf("sqlite: load transaction: %w", err)
	}
	var reads map[string]int64
	if err := json.Unmarshal([]byte(readsJSON), &reads); err != nil {
		return nil, fmt.Errorf("sqlite: decode transaction reads: %w", err)
	}

	now := time.Now().UTC()
	tx, err := a.rawDB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("sqlite: begin commit tx: %w", err)
	}

	// OCC check: verify no read document was modified since it was read.
	for path, version := range reads {
		var current int64
		err := tx.QueryRowContext(ctx, `SELECT version FROM documents WHERE path = ?`, path).Scan(&current)
		if errors.Is(err, sql.ErrNoRows) {
			_ = tx.Rollback()
			return nil, status.Errorf(codes.Aborted, "sqlite: transaction aborted: %s was deleted", path)
		}
		if err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("sqlite: occ check: %w", err)
		}
		if current != version {
			_ = tx.Rollback()
			return nil, status.Errorf(codes.Aborted,
				"sqlite: transaction aborted: %s version mismatch (read %d, current %d)",
				path, version, current)
		}
	}

	// Apply writes.
	results := make([]store.WriteResult, 0, len(ops))
	var changes []store.DocChange
	for _, op := range ops {
		switch op.Type {
		case store.WriteOpUpdate:
			d := op.Doc
			if d.UpdatedAt.IsZero() {
				d.UpdatedAt = now
			}
			if d.CreatedAt.IsZero() {
				d.CreatedAt = now
			}
			collection, parent := sqliteParseCollection(d.Path)
			updAt := d.UpdatedAt.UTC().Format(time.RFC3339Nano)
			crAt := d.CreatedAt.UTC().Format(time.RFC3339Nano)
			var execErr error
			switch op.Mode {
			case store.WriteModeUpsert:
				_, execErr = tx.ExecContext(ctx, `
                    INSERT INTO documents (path,collection,parent,data,created_at,updated_at,version)
                    VALUES (?,?,?,?,?,?,1)
                    ON CONFLICT(path) DO UPDATE SET
                        data=excluded.data, updated_at=excluded.updated_at,
                        version=documents.version+1`,
					d.Path, collection, parent, d.Data, crAt, updAt)
			case store.WriteModeUpdate:
				_, execErr = tx.ExecContext(ctx,
					`UPDATE documents SET data=?,updated_at=?,version=version+1 WHERE path=?`,
					d.Data, updAt, d.Path)
			case store.WriteModeInsertOnly:
				_, execErr = tx.ExecContext(ctx, `
                    INSERT INTO documents (path,collection,parent,data,created_at,updated_at,version)
                    VALUES (?,?,?,?,?,?,1)`,
					d.Path, collection, parent, d.Data, crAt, updAt)
			default:
				_ = tx.Rollback()
				return nil, fmt.Errorf("sqlite: commit write: unknown write mode %d", op.Mode)
			}
			if execErr != nil {
				_ = tx.Rollback()
				return nil, fmt.Errorf("sqlite: commit write: %w", execErr)
			}
			results = append(results, store.WriteResult{UpdatedAt: now})
			changes = append(changes, store.DocChange{
				Path:       d.Path,
				Collection: collection,
				Parent:     parent,
				Kind:       store.DocChangeUpsert,
				Data:       d.Data,
			})

		case store.WriteOpDelete:
			if _, err := tx.ExecContext(ctx, `DELETE FROM documents WHERE path=?`, op.Path); err != nil {
				_ = tx.Rollback()
				return nil, fmt.Errorf("sqlite: commit delete: %w", err)
			}
			results = append(results, store.WriteResult{UpdatedAt: now})
			collection, parent := sqliteParseCollection(op.Path)
			changes = append(changes, store.DocChange{
				Path:       op.Path,
				Collection: collection,
				Parent:     parent,
				Kind:       store.DocChangeDelete,
			})

		default:
			_ = tx.Rollback()
			return nil, fmt.Errorf("sqlite: commit: unknown write op type %d", op.Type)
		}
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM transactions WHERE id=?`, txID); err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("sqlite: delete transaction: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sqlite: commit transaction tx: %w", err)
	}

	for _, c := range changes {
		a.notifySubscribers(c)
	}

	return &store.CommitResult{WriteResults: results, CommitTime: now}, nil
}

// RollbackTransaction deletes the transaction record without applying writes.
func (a *Adapter) RollbackTransaction(ctx context.Context, txID string) error {
	_, err := a.execer(ctx).ExecContext(ctx, `DELETE FROM transactions WHERE id=?`, txID)
	if err != nil {
		return fmt.Errorf("sqlite: rollback transaction: %w", err)
	}
	return nil
}

// SweepExpiredTransactions deletes transaction records whose expires_at is in the past.
func (a *Adapter) SweepExpiredTransactions(ctx context.Context) (int, error) {
	result, err := a.execer(ctx).ExecContext(ctx,
		`DELETE FROM transactions WHERE expires_at < ?`,
		time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, fmt.Errorf("sqlite: sweep transactions: %w", err)
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}
