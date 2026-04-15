package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	migratepostgres "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/petereon/firstyr/internal/store"
)

// Compile-time check that Adapter implements store.StorageAdapter.
var _ store.StorageAdapter = (*Adapter)(nil)

// Adapter implements store.StorageAdapter for PostgreSQL.
// Additional methods (document ops, queries, listeners) are added in Plans 2-4.
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

// ─── Document CRUD stubs (Plan 2 — implemented in a future task) ─────────────

// CreateDocument is not yet implemented for PostgreSQL.
func (a *Adapter) CreateDocument(_ context.Context, _ *store.Document) (*store.Document, error) {
	return nil, errors.New("postgres: CreateDocument not implemented")
}

// GetDocument is not yet implemented for PostgreSQL.
func (a *Adapter) GetDocument(_ context.Context, _ string) (*store.Document, error) {
	return nil, errors.New("postgres: GetDocument not implemented")
}

// UpdateDocument is not yet implemented for PostgreSQL.
func (a *Adapter) UpdateDocument(_ context.Context, _ *store.Document, _ store.WriteMode) (*store.Document, error) {
	return nil, errors.New("postgres: UpdateDocument not implemented")
}

// DeleteDocument is not yet implemented for PostgreSQL.
func (a *Adapter) DeleteDocument(_ context.Context, _ string, _ bool) error {
	return errors.New("postgres: DeleteDocument not implemented")
}

// ListDocuments is not yet implemented for PostgreSQL.
func (a *Adapter) ListDocuments(_ context.Context, _, _ string, _ int32, _ string) (*store.ListPage, error) {
	return nil, errors.New("postgres: ListDocuments not implemented")
}
