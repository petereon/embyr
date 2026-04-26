package server_test

import (
	"context"
	"testing"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"github.com/stretchr/testify/require"
)

// #VM1 — Commit (non-transaction path) must return exactly one WriteResult per
// Write — including VerifyMutation entries (Operation==nil). The Firebase SDK
// validates this match in `commit_response_handler.ts` and throws when the
// counts diverge.
//
// (The `verify` field that carries the path-to-check is not present in this
// repo's generated proto, so we test the length-contract sub-issue here.
// The matching tx-path test lives in handlers_test.go.)
func TestCommit_NonTx_VerifyMutation_ResultsAlignWithWrites(t *testing.T) {
	ts := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, ts)

	const target = "projects/p/databases/(default)/documents/vm_nontx/target"

	resp, err := client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{
			// Write 0: VerifyMutation (nil operation, precondition only).
			{
				CurrentDocument: &firestorev1.Precondition{
					ConditionType: &firestorev1.Precondition_Exists{Exists: true},
				},
			},
			// Write 1: a real update.
			{Operation: &firestorev1.Write_Update{Update: &firestorev1.Document{
				Name: target,
				Fields: map[string]*firestorev1.Value{
					"x": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 1}},
				},
			}}},
		},
	})
	require.NoError(t, err)
	// Firestore spec: one WriteResult per Write. Two writes → two results.
	require.Len(t, resp.GetWriteResults(), 2,
		"non-tx Commit must return one WriteResult per Write — including VerifyMutation entries")
	// The verify entry's WriteResult must have UpdateTime set to the commit time.
	require.NotNil(t, resp.GetWriteResults()[0].GetUpdateTime(),
		"verify result must carry a non-nil UpdateTime equal to the commit time")
	// And the real update must have been applied.
	got, err := client.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: target})
	require.NoError(t, err)
	require.Equal(t, int64(1), got.GetFields()["x"].GetIntegerValue())
}
