package server_test

import (
	"context"
	"io"
	"testing"
	"time"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
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
