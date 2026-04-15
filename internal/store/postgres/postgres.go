package postgres

import (
	"context"
	"database/sql"
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
func New(dsn, migrationsPath string) (*Adapter, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: open: %w", err)
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
