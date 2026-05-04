package server

import (
	"context"
	"time"

	firestorev1 "github.com/petereon/embyr/gen/go/google/firestore/v1"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BeginTransaction creates a new server-side transaction and returns its ID.
func (s *firestoreServer) BeginTransaction(ctx context.Context, req *firestorev1.BeginTransactionRequest) (*firestorev1.BeginTransactionResponse, error) {
	readOnly := req.GetOptions().GetReadOnly() != nil
	txID, err := s.adapter(ctx).BeginTransaction(ctx, readOnly)
	if err != nil {
		return nil, err
	}
	return &firestorev1.BeginTransactionResponse{
		Transaction: []byte(txID),
	}, nil
}

// Rollback discards a transaction without applying writes.
func (s *firestoreServer) Rollback(ctx context.Context, req *firestorev1.RollbackRequest) (*emptypb.Empty, error) {
	txID := string(req.GetTransaction())
	if txID == "" {
		return nil, status.Error(codes.InvalidArgument, "transaction is required")
	}
	if err := s.adapter(ctx).RollbackTransaction(ctx, txID); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// BatchWrite applies a list of writes with per-write success/failure isolation.
// Per the Firestore spec, BatchWrite is NOT atomic — each write succeeds or
// fails independently and per-write errors are reported in the response's
// status array. Each write supports update_mask, update_transforms, and
// currentDocument preconditions; the implementation reuses applyWriteBatch
// per write so the semantics match Commit's non-tx path.
func (s *firestoreServer) BatchWrite(ctx context.Context, req *firestorev1.BatchWriteRequest) (*firestorev1.BatchWriteResponse, error) {
	now := time.Now().UTC()
	writes := req.GetWrites()
	writeResults := make([]*firestorev1.WriteResult, len(writes))
	statuses := make([]*rpcstatus.Status, len(writes))

	for i, w := range writes {
		// Apply each write in its own transaction so a failure doesn't roll
		// back its siblings.
		var perWriteResults []*firestorev1.WriteResult
		err := s.adapter(ctx).WithTransaction(ctx, func(txCtx context.Context) error {
			rs, batchErr := s.applyWriteBatch(txCtx, []*firestorev1.Write{w}, now)
			perWriteResults = rs
			return batchErr
		})
		if err != nil {
			st, ok := status.FromError(err)
			if !ok {
				st = status.New(codes.Unknown, err.Error())
			}
			statuses[i] = &rpcstatus.Status{
				Code:    int32(st.Code()),
				Message: st.Message(),
			}
			// Provide an empty (non-nil) WriteResult to keep slice positions aligned.
			writeResults[i] = &firestorev1.WriteResult{}
			continue
		}
		// applyWriteBatch returns one result per write; for a single write
		// it's a slice of length 0 (VerifyMutation no-op) or 1.
		if len(perWriteResults) > 0 {
			writeResults[i] = perWriteResults[0]
		} else {
			writeResults[i] = &firestorev1.WriteResult{UpdateTime: timestamppb.New(now)}
		}
		// nil entry = OK per Firestore spec.
		statuses[i] = nil
	}

	return &firestorev1.BatchWriteResponse{
		WriteResults: writeResults,
		Status:       statuses,
	}, nil
}
