package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"

	"github.com/golang-migrate/migrate/v4"
	migratepostgres "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// validSchema matches safe schema identifiers to prevent SQL injection.
var validSchema = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// Runner applies schema-namespaced migrations to a customer's Postgres DB.
// It uses golang-migrate with the schema name set so that all tables and the
// migration tracking table land in the target schema, not public.
type Runner struct {
	migrationsPath string
}

// NewRunner returns a Runner that reads SQL files from migrationsPath.
func NewRunner(migrationsPath string) *Runner {
	return &Runner{migrationsPath: migrationsPath}
}

// Up creates the schema if absent, then applies all pending migrations.
func (r *Runner) Up(ctx context.Context, db *sql.DB, schemaName string) error {
	if !validSchema.MatchString(schemaName) {
		return fmt.Errorf("migrations: invalid schema name %q", schemaName)
	}
	if _, err := db.ExecContext(ctx,
		`CREATE SCHEMA IF NOT EXISTS "`+schemaName+`"`); err != nil {
		return fmt.Errorf("migrations: create schema %q: %w", schemaName, err)
	}
	m, err := r.newMigrate(db, schemaName)
	if err != nil {
		return err
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrations: up %q: %w", schemaName, err)
	}
	return nil
}

// Down drops the schema and all tables within it.
func (r *Runner) Down(ctx context.Context, db *sql.DB, schemaName string) error {
	if !validSchema.MatchString(schemaName) {
		return fmt.Errorf("migrations: invalid schema name %q", schemaName)
	}
	if _, err := db.ExecContext(ctx,
		`DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`); err != nil {
		return fmt.Errorf("migrations: drop schema %q: %w", schemaName, err)
	}
	return nil
}

func (r *Runner) newMigrate(db *sql.DB, schemaName string) (*migrate.Migrate, error) {
	driver, err := migratepostgres.WithInstance(db, &migratepostgres.Config{
		SchemaName:      schemaName,
		MigrationsTable: "schema_migrations",
	})
	if err != nil {
		return nil, fmt.Errorf("migrations: driver for %q: %w", schemaName, err)
	}
	m, err := migrate.NewWithDatabaseInstance(
		"file://"+r.migrationsPath, "postgres", driver)
	if err != nil {
		return nil, fmt.Errorf("migrations: init for %q: %w", schemaName, err)
	}
	return m, nil
}
