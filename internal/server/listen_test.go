package server_test

import (
	"context"
	"io"
	"runtime"
	"testing"
	"time"

	firestorev1 "github.com/petereon/embyr/gen/go/google/firestore/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestListen_InitialSnapshot(t *testing.T) {
	srv := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gc, err := grpc.NewClient(srv.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer gc.Close()

	client := firestorev1.NewFirestoreClient(gc)

	// Seed a document
	_, err = client.CreateDocument(ctx, &firestorev1.CreateDocumentRequest{
		Parent:       "projects/p/databases/(default)/documents",
		CollectionId: "live",
		DocumentId:   "doc1",
		Document: &firestorev1.Document{
			Fields: map[string]*firestorev1.Value{
				"msg": {ValueType: &firestorev1.Value_StringValue{StringValue: "hello"}},
			},
		},
	})
	require.NoError(t, err)

	stream, err := client.Listen(ctx)
	require.NoError(t, err)

	// Send AddTarget for the "live" collection.
	err = stream.Send(&firestorev1.ListenRequest{
		Database: "projects/p/databases/(default)",
		TargetChange: &firestorev1.ListenRequest_AddTarget{
			AddTarget: &firestorev1.Target{
				TargetType: &firestorev1.Target_Query{
					Query: &firestorev1.Target_QueryTarget{
						Parent: "projects/p/databases/(default)/documents",
						QueryType: &firestorev1.Target_QueryTarget_StructuredQuery{
							StructuredQuery: &firestorev1.StructuredQuery{
								From: []*firestorev1.StructuredQuery_CollectionSelector{
									{CollectionId: "live"},
								},
							},
						},
					},
				},
				TargetId: 2,
			},
		},
	})
	require.NoError(t, err)

	// Expect: ADD target_change, then document_change(s), then CURRENT target_change.
	gotDocChange := false
	gotCurrent := false
	for !gotCurrent {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		switch r := resp.GetResponseType().(type) {
		case *firestorev1.ListenResponse_DocumentChange:
			require.Equal(t, "projects/p/databases/(default)/documents/live/doc1",
				r.DocumentChange.GetDocument().GetName())
			gotDocChange = true
		case *firestorev1.ListenResponse_TargetChange:
			if r.TargetChange.GetTargetChangeType() == firestorev1.TargetChange_CURRENT {
				gotCurrent = true
			}
		}
	}
	require.True(t, gotDocChange, "expected at least one DocumentChange in snapshot")
	require.True(t, gotCurrent, "expected CURRENT TargetChange after snapshot")
}

// TestListen_GoroutineCleanup verifies that goroutines spawned by the Listen
// handler (recv goroutine + registry subscription goroutine) are released when
// the client stream is closed. We measure goroutine count before and after an
// open+close cycle and assert it returns to within the gRPC framework's own
// overhead (≤ baseline + 2).
func TestListen_GoroutineCleanup(t *testing.T) {
	srv := startTestServer(t)

	// Open and immediately close a connection to pre-warm gRPC framework goroutines
	// so they don't pollute the baseline.
	warmGC, err := grpc.NewClient(srv.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	warmClient := firestorev1.NewFirestoreClient(warmGC)
	_, _ = warmClient.GetDocument(context.Background(), &firestorev1.GetDocumentRequest{
		Name: "projects/p/databases/(default)/documents/warmup/x",
	})
	warmGC.Close()

	// Allow the scheduler to drain framework goroutines.
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	baseline := runtime.NumGoroutine()

	// Open a dedicated connection and Listen stream.
	gc, err := grpc.NewClient(srv.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	client := firestorev1.NewFirestoreClient(gc)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.Listen(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&firestorev1.ListenRequest{
		Database: "projects/p/databases/(default)",
		TargetChange: &firestorev1.ListenRequest_AddTarget{
			AddTarget: &firestorev1.Target{
				TargetId: 2,
				TargetType: &firestorev1.Target_Query{
					Query: &firestorev1.Target_QueryTarget{
						Parent: "projects/p/databases/(default)/documents",
						QueryType: &firestorev1.Target_QueryTarget_StructuredQuery{
							StructuredQuery: &firestorev1.StructuredQuery{
								From: []*firestorev1.StructuredQuery_CollectionSelector{{CollectionId: "gc-test"}},
							},
						},
					},
				},
			},
		},
	}))

	// Drain until CURRENT so the stream is fully active.
	for {
		resp, err := stream.Recv()
		require.NoError(t, err)
		if tc, ok := resp.GetResponseType().(*firestorev1.ListenResponse_TargetChange); ok {
			if tc.TargetChange.GetTargetChangeType() == firestorev1.TargetChange_CURRENT {
				break
			}
		}
	}

	// Tear down: cancel context then close the gRPC connection to release
	// all client-side and server-side goroutines associated with this stream.
	cancel()
	gc.Close()

	// Wait for goroutines to drain back to baseline.
	require.Eventually(t, func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= baseline+2
	}, 3*time.Second, 50*time.Millisecond, "goroutines did not return to baseline after stream close")

	assert.LessOrEqual(t, runtime.NumGoroutine(), baseline+2)
}

func TestListen_DuplicateTargetId(t *testing.T) {
	srv := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	gc, err := grpc.NewClient(srv.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer gc.Close()
	client := firestorev1.NewFirestoreClient(gc)

	stream, err := client.Listen(ctx)
	require.NoError(t, err)

	addTarget := &firestorev1.ListenRequest{
		Database: "projects/p/databases/(default)",
		TargetChange: &firestorev1.ListenRequest_AddTarget{
			AddTarget: &firestorev1.Target{
				TargetId: 4,
				TargetType: &firestorev1.Target_Query{
					Query: &firestorev1.Target_QueryTarget{
						Parent: "projects/p/databases/(default)/documents",
						QueryType: &firestorev1.Target_QueryTarget_StructuredQuery{
							StructuredQuery: &firestorev1.StructuredQuery{
								From: []*firestorev1.StructuredQuery_CollectionSelector{{CollectionId: "dup-test"}},
							},
						},
					},
				},
			},
		},
	}

	require.NoError(t, stream.Send(addTarget))
	// Drain until CURRENT so first target is fully registered.
	for {
		resp, err := stream.Recv()
		require.NoError(t, err)
		if tc, ok := resp.GetResponseType().(*firestorev1.ListenResponse_TargetChange); ok {
			if tc.TargetChange.GetTargetChangeType() == firestorev1.TargetChange_CURRENT {
				break
			}
		}
	}

	// Send the same TargetId again — server must handle it non-fatally (real emulator
	// behaviour): send REMOVE for the old target, then re-deliver the snapshot.
	require.NoError(t, stream.Send(addTarget))

	gotRemove := false
	gotCurrent := false
	deadline := time.After(3 * time.Second)
	for !gotRemove || !gotCurrent {
		respCh := make(chan *firestorev1.ListenResponse, 1)
		errCh := make(chan error, 1)
		go func() {
			resp, err := stream.Recv()
			if err != nil {
				errCh <- err
			} else {
				respCh <- resp
			}
		}()
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for REMOVE+CURRENT after duplicate TargetId (gotRemove=%v gotCurrent=%v)", gotRemove, gotCurrent)
		case err := <-errCh:
			t.Fatalf("stream closed unexpectedly after duplicate TargetId: %v", err)
		case resp := <-respCh:
			tc, ok := resp.GetResponseType().(*firestorev1.ListenResponse_TargetChange)
			if !ok {
				continue
			}
			switch tc.TargetChange.GetTargetChangeType() {
			case firestorev1.TargetChange_REMOVE:
				gotRemove = true
			case firestorev1.TargetChange_CURRENT:
				gotCurrent = true
			}
		}
	}
}

func TestListen_DocumentTarget_InitialSnapshot(t *testing.T) {
	srv := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gc, err := grpc.NewClient(srv.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer gc.Close()
	client := firestorev1.NewFirestoreClient(gc)

	// Seed a document at a known path.
	_, err = client.CreateDocument(ctx, &firestorev1.CreateDocumentRequest{
		Parent:       "projects/p/databases/(default)/documents",
		CollectionId: "docwatch",
		DocumentId:   "d1",
		Document: &firestorev1.Document{
			Fields: map[string]*firestorev1.Value{
				"x": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 42}},
			},
		},
	})
	require.NoError(t, err)

	stream, err := client.Listen(ctx)
	require.NoError(t, err)

	docPath := "projects/p/databases/(default)/documents/docwatch/d1"
	require.NoError(t, stream.Send(&firestorev1.ListenRequest{
		Database: "projects/p/databases/(default)",
		TargetChange: &firestorev1.ListenRequest_AddTarget{
			AddTarget: &firestorev1.Target{
				TargetId: 2,
				TargetType: &firestorev1.Target_Documents{
					Documents: &firestorev1.Target_DocumentsTarget{
						Documents: []string{docPath},
					},
				},
			},
		},
	}))

	gotDoc := false
	gotCurrent := false
	for !gotCurrent {
		resp, err := stream.Recv()
		require.NoError(t, err)
		switch r := resp.GetResponseType().(type) {
		case *firestorev1.ListenResponse_DocumentChange:
			require.Equal(t, docPath, r.DocumentChange.GetDocument().GetName())
			gotDoc = true
		case *firestorev1.ListenResponse_TargetChange:
			if r.TargetChange.GetTargetChangeType() == firestorev1.TargetChange_CURRENT {
				gotCurrent = true
			}
		}
	}
	require.True(t, gotDoc, "expected DocumentChange for watched document")
	require.True(t, gotCurrent, "expected CURRENT after document target snapshot")
}

func TestListen_LiveChange(t *testing.T) {
	srv := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gc, err := grpc.NewClient(srv.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer gc.Close()
	client := firestorev1.NewFirestoreClient(gc)

	stream, err := client.Listen(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&firestorev1.ListenRequest{
		Database: "projects/p/databases/(default)",
		TargetChange: &firestorev1.ListenRequest_AddTarget{
			AddTarget: &firestorev1.Target{
				TargetType: &firestorev1.Target_Query{
					Query: &firestorev1.Target_QueryTarget{
						Parent: "projects/p/databases/(default)/documents",
						QueryType: &firestorev1.Target_QueryTarget_StructuredQuery{
							StructuredQuery: &firestorev1.StructuredQuery{
								From: []*firestorev1.StructuredQuery_CollectionSelector{{CollectionId: "live2"}},
							},
						},
					},
				},
				TargetId: 2,
			},
		},
	}))

	// Drain the initial snapshot (CURRENT).
	for {
		resp, err := stream.Recv()
		require.NoError(t, err)
		if tc, ok := resp.GetResponseType().(*firestorev1.ListenResponse_TargetChange); ok {
			if tc.TargetChange.GetTargetChangeType() == firestorev1.TargetChange_CURRENT {
				break
			}
		}
	}

	// Create a document AFTER the stream is established.
	_, err = client.CreateDocument(ctx, &firestorev1.CreateDocumentRequest{
		Parent:       "projects/p/databases/(default)/documents",
		CollectionId: "live2",
		DocumentId:   "new1",
		Document:     &firestorev1.Document{},
	})
	require.NoError(t, err)

	// Expect a DocumentChange for the new document.
	for {
		resp, err := stream.Recv()
		require.NoError(t, err)
		if dc, ok := resp.GetResponseType().(*firestorev1.ListenResponse_DocumentChange); ok {
			require.Contains(t, dc.DocumentChange.GetDocument().GetName(), "new1")
			return
		}
	}
}
