// cmd/embyr/main.go
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"path/filepath"

	"github.com/petereon/embyr/internal/config"
	"github.com/petereon/embyr/internal/server"
	"github.com/petereon/embyr/internal/store"
	"github.com/petereon/embyr/internal/store/postgres"
	"github.com/petereon/embyr/internal/store/sqlite"
	"go.uber.org/zap"
)

func main() {
	configPath := flag.String("config", "", "path to config YAML file (optional)")
	migrationsDir := flag.String("migrations-dir", "migrations", "directory containing migration subdirectories (sqlite/, postgres/)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("embyr: load config: %v", err)
	}

	var zapCfg zap.Config
	if cfg.Log.Format == "json" {
		zapCfg = zap.NewProductionConfig()
	} else {
		zapCfg = zap.NewDevelopmentConfig()
	}
	logger, err := zapCfg.Build()
	if err != nil {
		log.Fatalf("embyr: build logger: %v", err)
	}
	defer logger.Sync() //nolint:errcheck

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var adapter store.StorageAdapter

	switch cfg.Backend.Type {
	case "sqlite":
		a, err := sqlite.New(cfg.Backend.SQLite.Path, filepath.Join(*migrationsDir, "sqlite"))
		if err != nil {
			logger.Fatal("sqlite: open", zap.Error(err))
		}
		defer a.Close() //nolint:errcheck
		a.SetTransactionTTL(cfg.Transactions.TTL)
		adapter = a
	case "postgres":
		a, err := postgres.New(cfg.Backend.Postgres.DSN, filepath.Join(*migrationsDir, "postgres"), cfg.Backend.Postgres.MaxConns)
		if err != nil {
			logger.Fatal("postgres: open", zap.Error(err))
		}
		defer a.Close() //nolint:errcheck
		a.SetTransactionTTL(cfg.Transactions.TTL)
		adapter = a
	default:
		logger.Fatal("unknown backend type", zap.String("type", cfg.Backend.Type))
	}

	if err := adapter.Migrate(ctx); err != nil {
		logger.Fatal("migrate", zap.Error(err))
	}
	logger.Info("migrations applied")

	srv, err := server.New(cfg, adapter, logger)
	if err != nil {
		logger.Fatal("server: new", zap.Error(err))
	}

	logger.Info("embyr starting",
		zap.Int("grpc_port", cfg.Server.GRPCPort),
		zap.Int("rest_port", cfg.Server.RESTPort),
		zap.String("backend", cfg.Backend.Type),
		zap.String("auth", cfg.Auth.Mode),
	)

	if err := srv.Run(ctx); err != nil {
		logger.Fatal("server exited", zap.Error(err))
	}
}
