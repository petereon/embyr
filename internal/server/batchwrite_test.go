package server_test

import (
	"context"
	"testing"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"github.com/stretchr/testify/require"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// #BW1 — BatchWrite with update_mask must merge, not replace, the document.
func TestBatchWrite_MergeMask_PreservesUntouchedFields(t *testing.T) {
	ts := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, ts)

	const path = "projects/p/databases/(default)/documents/bw_merge/doc1"

	// Pre-create a document with two fields.
	_, err := client.CreateDocument(ctx, &firestorev1.CreateDocumentRequest{
		Parent:       "projects/p/databases/(default)/documents",
		CollectionId: "bw_merge",
		DocumentId:   "doc1",
		Document: &firestorev1.Document{
			Fields: map[string]*firestorev1.Value{
				"a": {ValueType: &firestorev1.Value_StringValue{StringValue: "alpha"}},
				"b": {ValueType: &firestorev1.Value_StringValue{StringValue: "beta"}},
			},
		},
	})
	require.NoError(t, err)

	// BatchWrite with mask=["b"] — must keep "a" intact.
	resp, err := client.BatchWrite(ctx, &firestorev1.BatchWriteRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{Update: &firestorev1.Document{
				Name: path,
				Fields: map[string]*firestorev1.Value{
					"b": {ValueType: &firestorev1.Value_StringValue{StringValue: "BETA"}},
				},
			}},
			UpdateMask: &firestorev1.DocumentMask{FieldPaths: []string{"b"}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetWriteResults(), 1)

	// Read back and confirm "a" is preserved.
	got, err := client.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: path})
	require.NoError(t, err)
	require.Equal(t, "alpha", got.GetFields()["a"].GetStringValue(),
		"BatchWrite with update_mask must merge — field 'a' must survive")
	require.Equal(t, "BETA", got.GetFields()["b"].GetStringValue())
}

// #BW1 — BatchWrite must apply update_transforms (serverTimestamp, increment).
func TestBatchWrite_AppliesTransforms_Increment(t *testing.T) {
	ts := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, ts)

	const path = "projects/p/databases/(default)/documents/bw_xform/doc1"

	_, err := client.CreateDocument(ctx, &firestorev1.CreateDocumentRequest{
		Parent:       "projects/p/databases/(default)/documents",
		CollectionId: "bw_xform",
		DocumentId:   "doc1",
		Document: &firestorev1.Document{Fields: map[string]*firestorev1.Value{
			"counter": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 10}},
		}},
	})
	require.NoError(t, err)

	// BatchWrite with increment(+5) transform on "counter".
	_, err = client.BatchWrite(ctx, &firestorev1.BatchWriteRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{Update: &firestorev1.Document{Name: path}},
			UpdateMask: &firestorev1.DocumentMask{FieldPaths: []string{}},
			UpdateTransforms: []*firestorev1.DocumentTransform_FieldTransform{{
				FieldPath: "counter",
				TransformType: &firestorev1.DocumentTransform_FieldTransform_Increment{
					Increment: &firestorev1.Value{ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 5}},
				},
			}},
		}},
	})
	require.NoError(t, err)

	got, err := client.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: path})
	require.NoError(t, err)
	require.Equal(t, int64(15), got.GetFields()["counter"].GetIntegerValue(),
		"increment transform must add to current value")
}

// #BW1 — BatchWrite must honor currentDocument.exists=true precondition.
func TestBatchWrite_HonorsPrecondition_MustExist(t *testing.T) {
	ts := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, ts)

	resp, err := client.BatchWrite(ctx, &firestorev1.BatchWriteRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{Update: &firestorev1.Document{
				Name: "projects/p/databases/(default)/documents/bw_pre/missing",
				Fields: map[string]*firestorev1.Value{
					"x": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 1}},
				},
			}},
			CurrentDocument: &firestorev1.Precondition{
				ConditionType: &firestorev1.Precondition_Exists{Exists: true},
			},
		}},
	})
	// Per-write semantics: top-level call succeeds, error reported in status[i].
	require.NoError(t, err)
	require.Len(t, resp.GetStatus(), 1)
	st := resp.GetStatus()[0]
	require.NotNil(t, st, "must-exist precondition on missing doc must yield a non-nil status entry")
	require.Equal(t, int32(codes.NotFound), st.GetCode(),
		"missing doc + Exists=true precondition must report NotFound")
}

// #BW2 — BatchWrite is per-write success/fail. A failing write must not abort the rest.
func TestBatchWrite_PerWriteIsolation_OneFailureDoesNotAbortBatch(t *testing.T) {
	ts := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, ts)

	resp, err := client.BatchWrite(ctx, &firestorev1.BatchWriteRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{
			// Write 0: succeeds.
			{Operation: &firestorev1.Write_Update{Update: &firestorev1.Document{
				Name: "projects/p/databases/(default)/documents/bw_iso/ok1",
				Fields: map[string]*firestorev1.Value{
					"x": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 1}},
				},
			}}},
			// Write 1: fails — currentDocument.exists=true on a missing doc.
			{
				Operation: &firestorev1.Write_Update{Update: &firestorev1.Document{
					Name: "projects/p/databases/(default)/documents/bw_iso/fail",
					Fields: map[string]*firestorev1.Value{
						"x": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 2}},
					},
				}},
				CurrentDocument: &firestorev1.Precondition{
					ConditionType: &firestorev1.Precondition_Exists{Exists: true},
				},
			},
			// Write 2: succeeds.
			{Operation: &firestorev1.Write_Update{Update: &firestorev1.Document{
				Name: "projects/p/databases/(default)/documents/bw_iso/ok2",
				Fields: map[string]*firestorev1.Value{
					"x": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 3}},
				},
			}}},
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetWriteResults(), 3)
	require.Len(t, resp.GetStatus(), 3)

	// Write 0 succeeded.
	require.True(t, isOKStatus(resp.GetStatus()[0]),
		"write 0 must succeed; got %+v", resp.GetStatus()[0])
	// Write 1 failed.
	require.False(t, isOKStatus(resp.GetStatus()[1]),
		"write 1 must fail; got %+v", resp.GetStatus()[1])
	require.Equal(t, int32(codes.NotFound), resp.GetStatus()[1].GetCode())
	// Write 2 succeeded.
	require.True(t, isOKStatus(resp.GetStatus()[2]),
		"write 2 must succeed; got %+v", resp.GetStatus()[2])

	// Confirm by reading back: ok1 and ok2 exist, fail does not.
	_, err = client.GetDocument(ctx, &firestorev1.GetDocumentRequest{
		Name: "projects/p/databases/(default)/documents/bw_iso/ok1",
	})
	require.NoError(t, err, "ok1 must have been written even though a sibling failed")
	_, err = client.GetDocument(ctx, &firestorev1.GetDocumentRequest{
		Name: "projects/p/databases/(default)/documents/bw_iso/ok2",
	})
	require.NoError(t, err, "ok2 must have been written even though a sibling failed")

	// And the WriteResult's UpdateTime must be set on success entries.
	require.NotNil(t, resp.GetWriteResults()[0].GetUpdateTime())
	require.NotNil(t, resp.GetWriteResults()[2].GetUpdateTime())

	// Avoid go vet "unused" complaints for timestamppb import.
	_ = timestamppb.Now
}

// isOKStatus reports whether s represents success (nil or code=OK=0).
func isOKStatus(s *rpcstatus.Status) bool {
	return s == nil || s.GetCode() == 0
}
