package server

import (
	"context"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"github.com/petereon/firstyr/internal/codec"
	"github.com/petereon/firstyr/internal/store"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BeginTransaction creates a new server-side transaction and returns its ID.
func (s *firestoreServer) BeginTransaction(ctx context.Context, req *firestorev1.BeginTransactionRequest) (*firestorev1.BeginTransactionResponse, error) {
	readOnly := req.GetOptions().GetReadOnly() != nil
	txID, err := s.db.BeginTransaction(ctx, readOnly)
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
	if err := s.db.RollbackTransaction(ctx, txID); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// BatchWrite applies a list of writes atomically using WithTransaction.
// Unlike Commit, BatchWrite does not use Firestore transactions or OCC.
func (s *firestoreServer) BatchWrite(ctx context.Context, req *firestorev1.BatchWriteRequest) (*firestorev1.BatchWriteResponse, error) {
	now := timestamppb.Now()
	writeResults := make([]*firestorev1.WriteResult, len(req.GetWrites()))

	err := s.db.WithTransaction(ctx, func(ctx context.Context) error {
		for i, w := range req.GetWrites() {
			switch op := w.GetOperation().(type) {
			case *firestorev1.Write_Update:
				doc := op.Update
				if doc.GetName() == "" {
					return status.Error(codes.InvalidArgument, "write.update.name is required")
				}
				mode := store.WriteModeUpsert
				if mask := w.GetUpdateMask(); mask != nil && len(mask.GetFieldPaths()) > 0 {
					mode = store.WriteModeUpdate
				}
				sd, err := codec.ProtoToStore(doc)
				if err != nil {
					return status.Errorf(codes.Internal, "encode document: %v", err)
				}
				result, err := s.db.UpdateDocument(ctx, sd, mode)
				if err != nil {
					return err
				}
				writeResults[i] = &firestorev1.WriteResult{UpdateTime: timestamppb.New(result.UpdatedAt)}

			case *firestorev1.Write_Delete:
				if op.Delete == "" {
					return status.Error(codes.InvalidArgument, "write.delete path is required")
				}
				mustExist := false
				if pre := w.GetCurrentDocument(); pre != nil {
					switch c := pre.GetConditionType().(type) {
					case *firestorev1.Precondition_Exists:
						mustExist = c.Exists
					case *firestorev1.Precondition_UpdateTime:
						mustExist = true
					}
				}
				if err := s.db.DeleteDocument(ctx, op.Delete, mustExist); err != nil {
					return err
				}
				writeResults[i] = &firestorev1.WriteResult{UpdateTime: now}

			default:
				return status.Error(codes.Unimplemented, "write operation type not supported in BatchWrite")
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// BatchWriteResponse.Status is []*google.rpc.Status; nil entries = success.
	statusSlice := make([]*rpcstatus.Status, len(writeResults))
	return &firestorev1.BatchWriteResponse{
		WriteResults: writeResults,
		Status:       statusSlice,
	}, nil
}
