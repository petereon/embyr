package server_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/petereon/firstyr/internal/config"
	"github.com/petereon/firstyr/internal/server"
	"github.com/petereon/firstyr/internal/store/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func TestServer_StartsAndServes(t *testing.T) {
	dir := t.TempDir()
	adapter, err := sqlite.New(dir+"/test.db", "../../migrations/sqlite")
	require.NoError(t, err)
	require.NoError(t, adapter.Migrate(context.Background()))
	defer adapter.Close()

	grpcPort := freePort(t)
	restPort := freePort(t)

	cfg := &config.Config{}
	cfg.Server.GRPCPort = grpcPort
	cfg.Server.RESTPort = restPort

	log, _ := zap.NewDevelopment()
	srv, err := server.New(cfg, adapter, log)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run(ctx) }()

	time.Sleep(150 * time.Millisecond)

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", restPort))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	cancel()
	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop within 2s")
	}
}
