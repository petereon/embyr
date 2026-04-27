package server_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"

	"github.com/petereon/embyr/internal/config"
	"github.com/petereon/embyr/internal/server"
	"github.com/petereon/embyr/internal/store/sqlite"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// #LISTENER-LEAK — When Run cannot allocate the REST listener (port already in
// use by another process), the gRPC listener that was successfully allocated
// must NOT leak. We block restPort with a foreign listener, then attempt to
// start the server. After Run returns the error, the gRPC port must be
// re-bindable.
func TestServer_GRPCListenerReleasedWhenRESTAllocFails(t *testing.T) {
	dir := t.TempDir()
	a, err := sqlite.New(filepath.Join(dir, "leak.db"), "../../migrations/sqlite")
	require.NoError(t, err)
	t.Cleanup(func() { a.Close() })
	require.NoError(t, a.Migrate(context.Background()))

	grpcPort := freePort(t)
	restPort := freePort(t)

	// Block the REST port with a foreign listener BEFORE Run gets to it.
	// Server.Run binds to ":<port>" (all interfaces) so we must do the same to
	// guarantee a conflict.
	blocker, err := net.Listen("tcp", fmt.Sprintf(":%d", restPort))
	require.NoError(t, err)
	defer blocker.Close()

	cfg := &config.Config{}
	cfg.Server.GRPCPort = grpcPort
	cfg.Server.RESTPort = restPort
	cfg.Auth.Mode = "none"

	log, _ := zap.NewDevelopment()
	srv, err := server.New(cfg, a, log)
	require.NoError(t, err)

	runErr := srv.Run(context.Background())
	require.Error(t, runErr, "Run must fail because REST port is in use")

	// After Run returns the error, the gRPC port must be free again.
	probe, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", grpcPort))
	require.NoError(t, err, "gRPC listener leaked when REST listener allocation failed")
	probe.Close()
}
