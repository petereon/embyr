package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	migratesqlite "github.com/golang-migrate/migrate/v4/database/sqlite"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "modernc.org/sqlite"

	"github.com/petereon/firstyr/internal/store"
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
	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: enable WAL: %w", err)
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
