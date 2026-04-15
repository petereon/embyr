package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	migratepostgres "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/petereon/firstyr/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Compile-time check that Adapter implements store.StorageAdapter.
var _ store.StorageAdapter = (*Adapter)(nil)

// Adapter implements store.StorageAdapter for PostgreSQL.
type Adapter struct {
	db             *sql.DB
	migrationsPath string
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
	return &Adapter{db: db, migrationsPath: migrationsPath}, nil
}

// Ping verifies the database connection is alive.
func (a *Adapter) Ping(ctx context.Context) error {
	return a.db.PingContext(ctx)
}

// Migrate applies all pending up-migrations from migrationsPath.
func (a *Adapter) Migrate(ctx context.Context) error {
	driver, err := migratepostgres.WithInstance(a.db, &migratepostgres.Config{})
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
	return a.db.Close()
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
	if len(relative) < 2 {
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
	row := a.db.QueryRowContext(ctx, `
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
	row := a.db.QueryRowContext(ctx,
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
		row := a.db.QueryRowContext(ctx, `
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
		row := a.db.QueryRowContext(ctx, `
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
		row := a.db.QueryRowContext(ctx, `
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
	result, err := a.db.ExecContext(ctx, `DELETE FROM documents WHERE path = $1`, path)
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
		rows, err = a.db.QueryContext(ctx,
			`SELECT path, data, created_at, updated_at, version
             FROM documents WHERE parent = $1
             ORDER BY path ASC LIMIT $2 OFFSET $3`,
			parent, fetchSize, offset)
	} else {
		rows, err = a.db.QueryContext(ctx,
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
