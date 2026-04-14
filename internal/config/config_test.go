package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/petereon/firstyr/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_Defaults(t *testing.T) {
	cfg, err := config.Load("")
	require.NoError(t, err)
	assert.Equal(t, 8080, cfg.Server.GRPCPort)
	assert.Equal(t, 8081, cfg.Server.RESTPort)
	assert.Equal(t, "none", cfg.Auth.Mode)
	assert.Equal(t, "sqlite", cfg.Backend.Type)
	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, "json", cfg.Log.Format)
	assert.Equal(t, 60*time.Second, cfg.Transactions.TTL)
	assert.Equal(t, 30*time.Second, cfg.Transactions.SweepInterval)
}

func TestLoad_FromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
server:
  grpc_port: 9090
  rest_port: 9091
auth:
  mode: key
  key: "secret"
backend:
  type: postgres
  postgres:
    dsn: "postgres://user:pass@localhost/db"
    max_conns: 10
`
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0600))

	cfg, err := config.Load(path)
	require.NoError(t, err)
	assert.Equal(t, 9090, cfg.Server.GRPCPort)
	assert.Equal(t, 9091, cfg.Server.RESTPort)
	assert.Equal(t, "key", cfg.Auth.Mode)
	assert.Equal(t, "secret", cfg.Auth.Key)
	assert.Equal(t, "postgres", cfg.Backend.Type)
	assert.Equal(t, "postgres://user:pass@localhost/db", cfg.Backend.Postgres.DSN)
	assert.Equal(t, 10, cfg.Backend.Postgres.MaxConns)
}

func TestLoad_EnvOverride(t *testing.T) {
	t.Setenv("FIRSTYR_AUTH_KEY", "env-override-key")
	t.Setenv("FIRSTYR_BACKEND_TYPE", "sqlite")

	cfg, err := config.Load("")
	require.NoError(t, err)
	assert.Equal(t, "env-override-key", cfg.Auth.Key)
	assert.Equal(t, "sqlite", cfg.Backend.Type)
}
