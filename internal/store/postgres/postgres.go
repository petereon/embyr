package postgres

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
	"time"

	"github.com/golang-migrate/migrate/v4"
	migratepostgres "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"

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

// Adapter implements store.StorageAdapter for PostgreSQL.
type Adapter struct {
	rawDB          *sql.DB   // for Ping, Close, BeginTx, migrations
	db             sqlExecer // for CRUD (never mutated after construction)
	migrationsPath string
	dsn            string // stored for pgx raw conn in Subscribe
}

// New opens a connection pool to the PostgreSQL database at dsn and returns a ready Adapter.
// maxConns sets the maximum number of open connections (0 = database/sql default, unlimited).
func New(dsn, migrationsPath string, maxConns int) (*Adapter, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: open: %w", err)
	}
	if maxConns > 0 {
		db.SetMaxOpenConns(maxConns)
	}
	return &Adapter{rawDB: db, db: db, migrationsPath: migrationsPath, dsn: dsn}, nil
}

// Ping verifies the database connection is alive.
func (a *Adapter) Ping(ctx context.Context) error {
	return a.rawDB.PingContext(ctx)
}

// Migrate applies all pending up-migrations from migrationsPath.
func (a *Adapter) Migrate(ctx context.Context) error {
	driver, err := migratepostgres.WithInstance(a.rawDB, &migratepostgres.Config{})
	if err != nil {
		return fmt.Errorf("postgres: migrate driver: %w", err)
	}
	m, err := migrate.NewWithDatabaseInstance(
		"file://"+a.migrationsPath,
		"postgres", driver,
	)
	if err != nil {
		return fmt.Errorf("postgres: migrate init: %w", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("postgres: migrate up: %w", err)
	}
	return nil
}

// Close releases the database connection pool.
func (a *Adapter) Close() error {
	return a.rawDB.Close()
}

// ─── Document CRUD ────────────────────────────────────────────────────────────

// pgParseCollection is a local duplicate of codec.ParsePath to avoid an
// import cycle (codec imports store; postgres is a sub-package of store).
func pgParseCollection(path string) (collection, parent string) {
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

// scanPgDoc scans a row from the PostgreSQL documents table.
// The data column is JSONB and comes back as []byte.
func scanPgDoc(row interface {
	Scan(dest ...any) error
}) (*store.Document, error) {
	var d store.Document
	var dataBytes []byte
	if err := row.Scan(&d.Path, &dataBytes, &d.CreatedAt, &d.UpdatedAt, &d.Version); err != nil {
		return nil, err
	}
	d.Data = string(dataBytes)
	d.CreatedAt = d.CreatedAt.UTC()
	d.UpdatedAt = d.UpdatedAt.UTC()
	return &d, nil
}

// isPgUniqueViolation reports whether err is a PostgreSQL unique constraint violation.
func isPgUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
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
	collection, parent := pgParseCollection(doc.Path)
	if collection == "" {
		return nil, status.Errorf(codes.InvalidArgument, "postgres: invalid document path: %s", doc.Path)
	}
	row := a.execer(ctx).QueryRowContext(ctx, `
		INSERT INTO documents (path, collection, parent, data, created_at, updated_at, version)
		VALUES ($1, $2, $3, $4::jsonb, $5, $6, 1)
		RETURNING path, data, created_at, updated_at, version`,
		doc.Path, collection, parent, doc.Data, doc.CreatedAt.UTC(), doc.UpdatedAt.UTC())
	d, err := scanPgDoc(row)
	if err != nil {
		if isPgUniqueViolation(err) {
			return nil, status.Errorf(codes.AlreadyExists, "postgres: document already exists: %s", doc.Path)
		}
		return nil, fmt.Errorf("postgres: create document: %w", err)
	}
	return d, nil
}

// GetDocument fetches a document by path. Returns codes.NotFound if absent.
func (a *Adapter) GetDocument(ctx context.Context, path string) (*store.Document, error) {
	row := a.execer(ctx).QueryRowContext(ctx,
		`SELECT path, data, created_at, updated_at, version FROM documents WHERE path = $1`, path)
	d, err := scanPgDoc(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "postgres: document not found: %s", path)
		}
		return nil, fmt.Errorf("postgres: get document: %w", err)
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

	switch mode {
	case store.WriteModeUpsert:
		collection, parent := pgParseCollection(doc.Path)
		if collection == "" {
			return nil, status.Errorf(codes.InvalidArgument, "postgres: invalid document path: %s", doc.Path)
		}
		row := a.execer(ctx).QueryRowContext(ctx, `
			INSERT INTO documents (path, collection, parent, data, created_at, updated_at, version)
			VALUES ($1, $2, $3, $4::jsonb, $5, $6, 1)
			ON CONFLICT(path) DO UPDATE SET
				data       = EXCLUDED.data,
				updated_at = EXCLUDED.updated_at,
				version    = documents.version + 1
			RETURNING path, data, created_at, updated_at, version`,
			doc.Path, collection, parent, doc.Data,
			doc.CreatedAt.UTC(), doc.UpdatedAt.UTC())
		d, err := scanPgDoc(row)
		if err != nil {
			return nil, fmt.Errorf("postgres: upsert document: %w", err)
		}
		return d, nil

	case store.WriteModeUpdate:
		row := a.execer(ctx).QueryRowContext(ctx, `
			UPDATE documents SET data=$1::jsonb, updated_at=$2, version=version+1
			WHERE path=$3
			RETURNING path, data, created_at, updated_at, version`,
			doc.Data, doc.UpdatedAt.UTC(), doc.Path)
		d, err := scanPgDoc(row)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, status.Errorf(codes.NotFound, "postgres: document not found: %s", doc.Path)
			}
			return nil, fmt.Errorf("postgres: update document: %w", err)
		}
		return d, nil

	case store.WriteModeInsertOnly:
		collection, parent := pgParseCollection(doc.Path)
		if collection == "" {
			return nil, status.Errorf(codes.InvalidArgument, "postgres: invalid document path: %s", doc.Path)
		}
		row := a.execer(ctx).QueryRowContext(ctx, `
			INSERT INTO documents (path, collection, parent, data, created_at, updated_at, version)
			VALUES ($1, $2, $3, $4::jsonb, $5, $6, 1)
			RETURNING path, data, created_at, updated_at, version`,
			doc.Path, collection, parent, doc.Data, doc.CreatedAt.UTC(), doc.UpdatedAt.UTC())
		d, err := scanPgDoc(row)
		if err != nil {
			if isPgUniqueViolation(err) {
				return nil, status.Errorf(codes.AlreadyExists, "postgres: document already exists: %s", doc.Path)
			}
			return nil, fmt.Errorf("postgres: insert-only document: %w", err)
		}
		return d, nil

	default:
		return nil, fmt.Errorf("postgres: unknown write mode %d", mode)
	}
}

// DeleteDocument removes a document. Returns codes.NotFound if mustExist and absent.
func (a *Adapter) DeleteDocument(ctx context.Context, path string, mustExist bool) error {
	result, err := a.execer(ctx).ExecContext(ctx, `DELETE FROM documents WHERE path = $1`, path)
	if err != nil {
		return fmt.Errorf("postgres: delete document: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres: delete rows affected: %w", err)
	}
	if mustExist && n == 0 {
		return status.Errorf(codes.NotFound, "postgres: document not found: %s", path)
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
             FROM documents WHERE parent = $1
             ORDER BY path ASC LIMIT $2 OFFSET $3`,
			parent, fetchSize, offset)
	} else {
		rows, err = a.execer(ctx).QueryContext(ctx,
			`SELECT path, data, created_at, updated_at, version
             FROM documents WHERE collection = $1 AND parent = $2
             ORDER BY path ASC LIMIT $3 OFFSET $4`,
			collectionID, parent, fetchSize, offset)
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: list documents: %w", err)
	}
	defer rows.Close()

	docs := make([]*store.Document, 0, pageSize)
	for rows.Next() {
		d, err := scanPgDoc(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: scan document row: %w", err)
		}
		docs = append(docs, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list documents rows: %w", err)
	}

	var nextToken string
	if len(docs) > int(pageSize) {
		docs = docs[:pageSize]
		nextToken = store.EncodePageToken(offset + int(pageSize))
	}
	return &store.ListPage{Documents: docs, NextPageToken: nextToken}, nil
}

// ─── Query helpers ────────────────────────────────────────────────────────────

// pgFieldBase returns the JSONB navigation to the field's value node.
// "name" → data->'fields'->'name'
// "a.b"  → data->'fields'->'a'->'mapValue'->'fields'->'b'
func pgFieldBase(fieldPath string) string {
	parts := strings.Split(fieldPath, ".")
	var sb strings.Builder
	sb.WriteString("data->'fields'")
	for i, p := range parts {
		sb.WriteString(fmt.Sprintf("->'%s'", p))
		if i < len(parts)-1 {
			sb.WriteString("->'mapValue'->'fields'")
		}
	}
	return sb.String()
}

// pgScalarExpr returns a SQL expression that extracts a typed scalar from JSONB.
func pgScalarExpr(fieldPath string, kind store.FilterValueKind) string {
	base := pgFieldBase(fieldPath)
	switch kind {
	case store.FilterValueString:
		return base + "->>'stringValue'"
	case store.FilterValueBool:
		return base + "->>'booleanValue'"
	case store.FilterValueTime:
		return base + "->>'timestampValue'"
	case store.FilterValueInt:
		return fmt.Sprintf("(%s->>'integerValue')::bigint", base)
	case store.FilterValueDouble:
		return fmt.Sprintf("(%s->>'doubleValue')::double precision", base)
	default:
		return base + "->>'stringValue'"
	}
}

// pgScalarArg returns the Go value to bind for a FilterValue.
func pgScalarArg(fv store.FilterValue) interface{} {
	switch fv.Kind {
	case store.FilterValueInt:
		return fv.IntVal
	case store.FilterValueDouble:
		return fv.DoubleVal
	case store.FilterValueBool:
		if fv.BoolVal {
			return "true"
		}
		return "false"
	case store.FilterValueTime:
		return fv.TimeVal.UTC().Format(time.RFC3339Nano)
	default:
		return fv.StrVal
	}
}

// pgFilterClause builds a SQL WHERE clause fragment for a single FieldFilter
// using numbered placeholders ($N). argN is incremented for each placeholder added.
func pgFilterClause(f store.FieldFilter, argN *int, args *[]interface{}) (string, error) {
	if err := validateFieldPath(f.Field); err != nil {
		return "", err
	}
	ph := func() string { *argN++; return fmt.Sprintf("$%d", *argN) }

	switch f.Op {
	case store.FilterOpEqual, store.FilterOpNotEqual,
		store.FilterOpLessThan, store.FilterOpLessThanOrEqual,
		store.FilterOpGreaterThan, store.FilterOpGreaterThanOrEqual:
		expr := pgScalarExpr(f.Field, f.Value.Kind)
		opMap := map[store.FilterOp]string{
			store.FilterOpEqual:              "=",
			store.FilterOpNotEqual:           "<>",
			store.FilterOpLessThan:           "<",
			store.FilterOpLessThanOrEqual:    "<=",
			store.FilterOpGreaterThan:        ">",
			store.FilterOpGreaterThanOrEqual: ">=",
		}
		*args = append(*args, pgScalarArg(f.Value))
		return fmt.Sprintf("%s %s %s", expr, opMap[f.Op], ph()), nil

	case store.FilterOpIn, store.FilterOpNotIn:
		expr := pgScalarExpr(f.Field, f.Value.Kind)
		phs := make([]string, len(f.Value.ArrayVals))
		for i, v := range f.Value.ArrayVals {
			*args = append(*args, pgScalarArg(v))
			phs[i] = ph()
		}
		op := "IN"
		if f.Op == store.FilterOpNotIn {
			op = "NOT IN"
		}
		return fmt.Sprintf("%s %s (%s)", expr, op, strings.Join(phs, ",")), nil

	case store.FilterOpArrayContains:
		base := pgFieldBase(f.Field)
		arrayExpr := base + "->'arrayValue'->'values'"
		typeKey := map[store.FilterValueKind]string{
			store.FilterValueString: "stringValue",
			store.FilterValueInt:    "integerValue",
			store.FilterValueDouble: "doubleValue",
			store.FilterValueBool:   "booleanValue",
		}[f.Value.Kind]
		*args = append(*args, pgScalarArg(f.Value))
		return fmt.Sprintf(
			"EXISTS (SELECT 1 FROM jsonb_array_elements(%s) AS elem WHERE elem->>'%s' = %s)",
			arrayExpr, typeKey, ph()), nil

	default:
		return "", status.Errorf(codes.Unimplemented, "filter op %v not supported in postgres", f.Op)
	}
}

// pgOrderExpr builds an ORDER BY expression for a single OrderBy clause.
func pgOrderExpr(o store.OrderBy) (string, error) {
	if o.Direction != store.DirectionAsc && o.Direction != store.DirectionDesc {
		return "", status.Errorf(codes.InvalidArgument, "invalid order direction: %q", o.Direction)
	}
	if err := validateFieldPath(o.Field); err != nil {
		return "", err
	}
	base := pgFieldBase(o.Field)
	return fmt.Sprintf("%s %s", base, string(o.Direction)), nil
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
	argN := 0
	ph := func() string { argN++; return fmt.Sprintf("$%d", argN) }

	args = append(args, q.Parent)
	sb.WriteString(`SELECT path, data, created_at, updated_at, version FROM documents WHERE parent = ` + ph())

	if q.CollectionID != "" {
		args = append(args, q.CollectionID)
		sb.WriteString(` AND collection = ` + ph())
	}

	if q.Filter != nil {
		for _, f := range q.Filter.Filters {
			clause, err := pgFilterClause(f, &argN, &args)
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
			expr, err := pgOrderExpr(o)
			if err != nil {
				return nil, err
			}
			sb.WriteString(expr)
		}
	} else {
		sb.WriteString(" ORDER BY path ASC")
	}

	args = append(args, fetchSize, offset)
	sb.WriteString(fmt.Sprintf(" LIMIT %s OFFSET %s", ph(), ph()))

	rows, err := a.execer(ctx).QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: query documents: %w", err)
	}
	defer rows.Close()

	docs := make([]*store.Document, 0, pageSize)
	for rows.Next() {
		d, err := scanPgDoc(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: scan query row: %w", err)
		}
		docs = append(docs, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: query rows: %w", err)
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
// Adapter do not race on a shared field.
func (a *Adapter) WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := a.rawDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres: begin tx: %w", err)
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
func (a *Adapter) BeginTransaction(ctx context.Context, readOnly bool) (string, error) {
	_ = readOnly // reserved for future read-only enforcement
	id := newTxID()
	t := time.Now().UTC()
	expires := t.Add(60 * time.Second)
	_, err := a.execer(ctx).ExecContext(ctx,
		`INSERT INTO transactions (id, started_at, reads, expires_at) VALUES ($1, $2, '{}', $3)`,
		id, t, expires)
	if err != nil {
		return "", fmt.Errorf("postgres: begin transaction: %w", err)
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
		`UPDATE transactions SET reads = reads || jsonb_build_object($1::text, $2::bigint) WHERE id = $3`,
		path, doc.Version, txID)
	if err != nil {
		return nil, fmt.Errorf("postgres: record transaction read: %w", err)
	}
	return doc, nil
}

// CommitTransaction verifies OCC, applies ops atomically, and deletes the
// transaction record.
func (a *Adapter) CommitTransaction(ctx context.Context, txID string, ops []store.WriteOp) (*store.CommitResult, error) {
	var readsJSON []byte
	err := a.rawDB.QueryRowContext(ctx,
		`SELECT reads FROM transactions WHERE id = $1`, txID).Scan(&readsJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "postgres: transaction not found: %s", txID)
		}
		return nil, fmt.Errorf("postgres: load transaction: %w", err)
	}
	var reads map[string]int64
	if err := json.Unmarshal(readsJSON, &reads); err != nil {
		return nil, fmt.Errorf("postgres: decode transaction reads: %w", err)
	}

	now := time.Now().UTC()
	tx, err := a.rawDB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("postgres: begin commit tx: %w", err)
	}

	// OCC check: verify no read document was modified since it was read.
	for path, version := range reads {
		var current int64
		err := tx.QueryRowContext(ctx, `SELECT version FROM documents WHERE path = $1`, path).Scan(&current)
		if errors.Is(err, sql.ErrNoRows) {
			_ = tx.Rollback()
			return nil, status.Errorf(codes.Aborted, "postgres: transaction aborted: %s was deleted", path)
		}
		if err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("postgres: occ check: %w", err)
		}
		if current != version {
			_ = tx.Rollback()
			return nil, status.Errorf(codes.Aborted,
				"postgres: transaction aborted: %s version mismatch (read %d, current %d)",
				path, version, current)
		}
	}

	// Apply writes.
	results := make([]store.WriteResult, 0, len(ops))
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
			collection, parent := pgParseCollection(d.Path)
			var execErr error
			switch op.Mode {
			case store.WriteModeUpsert:
				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO documents (path,collection,parent,data,created_at,updated_at,version)
					VALUES ($1,$2,$3,$4::jsonb,$5,$6,1)
					ON CONFLICT(path) DO UPDATE SET
						data=EXCLUDED.data, updated_at=EXCLUDED.updated_at,
						version=documents.version+1`,
					d.Path, collection, parent, d.Data, d.CreatedAt, d.UpdatedAt)
			case store.WriteModeUpdate:
				_, execErr = tx.ExecContext(ctx,
					`UPDATE documents SET data=$1::jsonb,updated_at=$2,version=version+1 WHERE path=$3`,
					d.Data, d.UpdatedAt, d.Path)
			case store.WriteModeInsertOnly:
				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO documents (path,collection,parent,data,created_at,updated_at,version)
					VALUES ($1,$2,$3,$4::jsonb,$5,$6,1)`,
					d.Path, collection, parent, d.Data, d.CreatedAt, d.UpdatedAt)
			default:
				_ = tx.Rollback()
				return nil, status.Errorf(codes.InvalidArgument, "postgres: unknown write mode: %v", op.Mode)
			}
			if execErr != nil {
				_ = tx.Rollback()
				return nil, fmt.Errorf("postgres: commit write: %w", execErr)
			}
			results = append(results, store.WriteResult{UpdatedAt: now})

		case store.WriteOpDelete:
			if _, err := tx.ExecContext(ctx, `DELETE FROM documents WHERE path=$1`, op.Path); err != nil {
				_ = tx.Rollback()
				return nil, fmt.Errorf("postgres: commit delete: %w", err)
			}
			results = append(results, store.WriteResult{UpdatedAt: now})

		default:
			_ = tx.Rollback()
			return nil, status.Errorf(codes.InvalidArgument, "postgres: unknown write op type: %v", op.Type)
		}
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM transactions WHERE id=$1`, txID); err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("postgres: delete transaction: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("postgres: commit transaction tx: %w", err)
	}
	return &store.CommitResult{WriteResults: results, CommitTime: now}, nil
}

// RollbackTransaction deletes the transaction record without applying writes.
func (a *Adapter) RollbackTransaction(ctx context.Context, txID string) error {
	_, err := a.execer(ctx).ExecContext(ctx, `DELETE FROM transactions WHERE id=$1`, txID)
	if err != nil {
		return fmt.Errorf("postgres: rollback transaction: %w", err)
	}
	return nil
}

// SweepExpiredTransactions deletes transaction records whose expires_at is in the past.
func (a *Adapter) SweepExpiredTransactions(ctx context.Context) (int, error) {
	result, err := a.execer(ctx).ExecContext(ctx,
		`DELETE FROM transactions WHERE expires_at < NOW()`)
	if err != nil {
		return 0, fmt.Errorf("postgres: sweep transactions: %w", err)
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

// Subscribe opens a dedicated pgx connection, issues LISTEN doc_changes, and
// returns a channel that receives a DocChange for every committed document
// write. The returned cancel function tears down the listener and closes the
// channel. Subscribe is safe to call concurrently; each call opens its own
// connection.
func (a *Adapter) Subscribe() (<-chan store.DocChange, func()) {
	ch := make(chan store.DocChange, 64)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		defer close(ch)
		conn, err := pgx.Connect(ctx, a.dsn)
		if err != nil {
			return
		}
		defer conn.Close(ctx)

		if _, err := conn.Exec(ctx, "LISTEN doc_changes"); err != nil {
			return
		}

		for {
			notif, err := conn.WaitForNotification(ctx)
			if err != nil {
				return // context cancelled or connection lost
			}
			var payload struct {
				Path       string `json:"path"`
				Collection string `json:"collection"`
				Parent     string `json:"parent"`
				Kind       string `json:"kind"`
				Version    int64  `json:"version"`
				Data       string `json:"data"`
			}
			if err := json.Unmarshal([]byte(notif.Payload), &payload); err != nil {
				continue
			}
			kind := store.DocChangeUpsert
			if payload.Kind == "delete" {
				kind = store.DocChangeDelete
			}
			c := store.DocChange{
				Path:       payload.Path,
				Collection: payload.Collection,
				Parent:     payload.Parent,
				Kind:       kind,
				Version:    payload.Version,
				Data:       payload.Data,
			}
			select {
			case ch <- c:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, cancel
}
