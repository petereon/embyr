// cmd/data/main.go — multi-tenant data plane entry point for Cloud Run.
package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/petereon/embyr/internal/config"
	"github.com/petereon/embyr/internal/registry"
	"github.com/petereon/embyr/internal/server"
	"github.com/petereon/embyr/internal/tenancy"
	"go.uber.org/zap"
)

func main() {
	configPath := flag.String("config", "", "path to config YAML file (optional)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("data: load config: %v", err)
	}

	var zapCfg zap.Config
	if cfg.Log.Format == "json" {
		zapCfg = zap.NewProductionConfig()
	} else {
		zapCfg = zap.NewDevelopmentConfig()
	}
	logger, err := zapCfg.Build()
	if err != nil {
		log.Fatalf("data: build logger: %v", err)
	}
	defer logger.Sync() //nolint:errcheck

	registryDSN := os.Getenv("EMBYR_REGISTRY_DSN")
	if registryDSN == "" {
		logger.Fatal("data: EMBYR_REGISTRY_DSN env var required")
	}

	regDB, err := sql.Open("pgx", registryDSN)
	if err != nil {
		logger.Fatal("data: open registry DB", zap.Error(err))
	}
	defer regDB.Close()

	regClient := registry.NewPostgresClient(regDB, 60*time.Second)
	defer regClient.Close()

	factory := tenancy.NewAdapterFactory(regClient, 200).WithLogger(logger)
	defer factory.Close()

	srv, err := server.NewMultiTenant(cfg, factory, logger)
	if err != nil {
		logger.Fatal("data: server init", zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("embyr data plane starting",
		zap.Int("grpc_port", cfg.Server.GRPCPort),
		zap.Int("rest_port", cfg.Server.RESTPort),
	)

	if err := srv.Run(ctx); err != nil {
		logger.Fatal("data: server exited", zap.Error(err))
	}
}
