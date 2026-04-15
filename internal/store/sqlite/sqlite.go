package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
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

// Adapter implements store.StorageAdapter for SQLite.
// Additional methods (document ops, queries, listeners) are added in Plans 2-4.
type Adapter struct {
	db             *sql.DB
	migrationsPath string
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
	return &Adapter{db: db, migrationsPath: migrationsPath}, nil
}

// Ping verifies the database connection is alive.
func (a *Adapter) Ping(ctx context.Context) error {
	return a.db.PingContext(ctx)
}

// Migrate applies all pending up-migrations from migrationsPath.
func (a *Adapter) Migrate(ctx context.Context) error {
	driver, err := migratesqlite.WithInstance(a.db, &migratesqlite.Config{})
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
	return a.db.Close()
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
		if d.CreatedAt, err = time.Parse("2006-01-02T15:04:05.999999999-07:00", createdStr); err != nil {
			d.CreatedAt = time.Now().UTC()
		}
	}
	if d.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedStr); err != nil {
		if d.UpdatedAt, err = time.Parse("2006-01-02T15:04:05.999999999-07:00", updatedStr); err != nil {
			d.UpdatedAt = time.Now().UTC()
		}
	}
	return &d, nil
}

// CreateDocument inserts a new document. Returns codes.AlreadyExists if path taken.
func (a *Adapter) CreateDocument(ctx context.Context, doc *store.Document) (*store.Document, error) {
	collection, parent := sqliteParseCollection(doc.Path)
	now := doc.UpdatedAt.UTC().Format(time.RFC3339Nano)
	createdAt := doc.CreatedAt.UTC().Format(time.RFC3339Nano)

	row := a.db.QueryRowContext(ctx, sqlInsertDoc,
		doc.Path, collection, parent, doc.Data, createdAt, now)
	d, err := scanDoc(row)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return nil, status.Errorf(codes.AlreadyExists, "sqlite: document already exists: %s", doc.Path)
		}
		return nil, fmt.Errorf("sqlite: create document: %w", err)
	}
	return d, nil
}

// GetDocument fetches a document by path. Returns codes.NotFound if absent.
func (a *Adapter) GetDocument(ctx context.Context, path string) (*store.Document, error) {
	row := a.db.QueryRowContext(ctx, sqlGetDoc, path)
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
	now := doc.UpdatedAt.UTC().Format(time.RFC3339Nano)

	switch mode {
	case store.WriteModeUpsert:
		collection, parent := sqliteParseCollection(doc.Path)
		createdAt := doc.CreatedAt.UTC().Format(time.RFC3339Nano)
		row := a.db.QueryRowContext(ctx, sqlUpsertDoc,
			doc.Path, collection, parent, doc.Data, createdAt, now)
		d, err := scanDoc(row)
		if err != nil {
			return nil, fmt.Errorf("sqlite: upsert document: %w", err)
		}
		return d, nil

	case store.WriteModeUpdate:
		row := a.db.QueryRowContext(ctx, sqlUpdateDoc, doc.Data, now, doc.Path)
		d, err := scanDoc(row)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, status.Errorf(codes.NotFound, "sqlite: document not found: %s", doc.Path)
			}
			return nil, fmt.Errorf("sqlite: update document: %w", err)
		}
		return d, nil

	case store.WriteModeInsertOnly:
		collection, parent := sqliteParseCollection(doc.Path)
		createdAt := doc.CreatedAt.UTC().Format(time.RFC3339Nano)
		row := a.db.QueryRowContext(ctx, sqlInsertDoc,
			doc.Path, collection, parent, doc.Data, createdAt, now)
		d, err := scanDoc(row)
		if err != nil {
			if strings.Contains(err.Error(), "UNIQUE constraint failed") {
				return nil, status.Errorf(codes.AlreadyExists, "sqlite: document already exists: %s", doc.Path)
			}
			return nil, fmt.Errorf("sqlite: insert-only document: %w", err)
		}
		return d, nil

	default:
		return nil, fmt.Errorf("sqlite: unknown write mode %d", mode)
	}
}

// DeleteDocument removes a document. Returns codes.NotFound if mustExist and absent.
func (a *Adapter) DeleteDocument(ctx context.Context, path string, mustExist bool) error {
	result, err := a.db.ExecContext(ctx, sqlDeleteDoc, path)
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
	return nil
}

// ListDocuments returns documents in a collection, paginated by offset.
func (a *Adapter) ListDocuments(ctx context.Context, parent, collectionID string, pageSize int32, pageToken string) (*store.ListPage, error) {
	if pageSize <= 0 {
		pageSize = 100
	}
	offset := store.DecodePageToken(pageToken)

	var (
		rows *sql.Rows
		err  error
	)
	if collectionID == "" {
		rows, err = a.db.QueryContext(ctx,
			`SELECT path, data, created_at, updated_at, version
             FROM documents WHERE parent = ?
             ORDER BY path ASC LIMIT ? OFFSET ?`,
			parent, pageSize, offset)
	} else {
		rows, err = a.db.QueryContext(ctx,
			`SELECT path, data, created_at, updated_at, version
             FROM documents WHERE collection = ? AND parent = ?
             ORDER BY path ASC LIMIT ? OFFSET ?`,
			collectionID, parent, pageSize, offset)
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
	if int(pageSize) == len(docs) {
		nextToken = store.EncodePageToken(offset + int(pageSize))
	}
	return &store.ListPage{Documents: docs, NextPageToken: nextToken}, nil
}
