package server_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	firestorev1 "github.com/petereon/embyr/gen/go/google/firestore/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// #LISTENRECV — Listen recv goroutine must release even when its buffered send
// would otherwise block. Exercise the path by opening many short-lived Listen
// streams and sending more requests than the recv buffer can hold; if the
// recv goroutine leaks, the runtime goroutine count grows monotonically.
func TestListen_RecvGoroutine_NoLeakUnderBackpressure(t *testing.T) {
	srv := startTestServer(t)

	// Pre-warm the gRPC client framework goroutines.
	warm, err := grpc.NewClient(srv.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	warmClient := firestorev1.NewFirestoreClient(warm)
	_, _ = warmClient.GetDocument(context.Background(), &firestorev1.GetDocumentRequest{
		Name: "projects/p/databases/(default)/documents/warm/x",
	})
	warm.Close()
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	baseline := runtime.NumGoroutine()

	const cycles = 30
	const reqsPerCycle = 16 // > recvCh buffer (8) so the goroutine has to block-then-escape

	for i := 0; i < cycles; i++ {
		gc, err := grpc.NewClient(srv.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		client := firestorev1.NewFirestoreClient(gc)

		ctx, cancel := context.WithCancel(context.Background())
		stream, err := client.Listen(ctx)
		require.NoError(t, err)

		// Fire reqsPerCycle AddTargets fast — this overflows the server-side
		// recv buffer if the main loop is slow. If the recv goroutine has no
		// ctx.Done() escape on its send, the goroutine leaks here.
		for j := 0; j < reqsPerCycle; j++ {
			_ = stream.Send(&firestorev1.ListenRequest{
				Database: "projects/p/databases/(default)",
				TargetChange: &firestorev1.ListenRequest_AddTarget{
					AddTarget: &firestorev1.Target{
						TargetId: int32(j + 100),
						TargetType: &firestorev1.Target_Query{
							Query: &firestorev1.Target_QueryTarget{
								Parent: "projects/p/databases/(default)/documents",
								QueryType: &firestorev1.Target_QueryTarget_StructuredQuery{
									StructuredQuery: &firestorev1.StructuredQuery{
										From: []*firestorev1.StructuredQuery_CollectionSelector{
											{CollectionId: "leak-test"},
										},
									},
								},
							},
						},
					},
				},
			})
		}

		// Cancel without draining responses.
		cancel()
		gc.Close()
	}

	// Allow goroutines to drain.
	require.Eventually(t, func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= baseline+5
	}, 5*time.Second, 50*time.Millisecond,
		"goroutines did not return to baseline (baseline=%d, current=%d) — recv goroutine leak suspected",
		baseline, runtime.NumGoroutine())
}
