package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	firestorev1 "github.com/petereon/embyr/gen/go/google/firestore/v1"
	"github.com/petereon/embyr/internal/codec"
	"github.com/petereon/embyr/internal/listen"
	"github.com/petereon/embyr/internal/store"
	"github.com/petereon/embyr/internal/tenancy"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// firestoreServer is the gRPC service implementation.
type firestoreServer struct {
	firestorev1.UnimplementedFirestoreServer
	db       store.StorageAdapter
	log      *zap.Logger
	registry *listen.Registry
}

// adapter returns the StorageAdapter for this request.
// In multi-tenant mode the tenancy middleware injects a per-tenant adapter.
// In single-tenant mode it falls back to s.db.
func (s *firestoreServer) adapter(ctx context.Context) store.StorageAdapter {
	if a := tenancy.AdapterFromCtx(ctx); a != nil {
		return a
	}
	return s.db
}

// GetDocument fetches a single document by its resource name.
func (s *firestoreServer) GetDocument(ctx context.Context, req *firestorev1.GetDocumentRequest) (*firestorev1.Document, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	d, err := s.adapter(ctx).GetDocument(ctx, req.GetName())
	if err != nil {
		return nil, err
	}
	return codec.StoreToProto(d)
}

// CreateDocument creates a new document in a collection.
func (s *firestoreServer) CreateDocument(ctx context.Context, req *firestorev1.CreateDocumentRequest) (*firestorev1.Document, error) {
	if req.GetParent() == "" {
		return nil, status.Error(codes.InvalidArgument, "parent is required")
	}
	if req.GetCollectionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_id is required")
	}
	docID := req.GetDocumentId()
	if docID == "" {
		var err error
		docID, err = codec.NewDocumentID()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "generate document ID: %v", err)
		}
	}
	// The grpc-gateway may append a trailing slash to the parent path variable when
	// the ** wildcard in the URL pattern captures zero segments (e.g. top-level
	// collections). Normalise the parent before constructing the document path so
	// we never store paths with double slashes.
	parent := strings.TrimRight(req.GetParent(), "/")
	path := codec.BuildPath(parent, req.GetCollectionId(), docID)

	inDoc := req.GetDocument()
	writeProto := &firestorev1.Document{Name: path}
	if inDoc != nil {
		writeProto.Fields = inDoc.GetFields()
	}

	sd, err := codec.ProtoToStore(writeProto)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode document: %v", err)
	}
	created, err := s.adapter(ctx).CreateDocument(ctx, sd)
	if err != nil {
		return nil, err
	}
	return codec.StoreToProto(created)
}

// UpdateDocument updates or creates a document (upsert by default).
func (s *firestoreServer) UpdateDocument(ctx context.Context, req *firestorev1.UpdateDocumentRequest) (*firestorev1.Document, error) {
	doc := req.GetDocument()
	if doc == nil {
		return nil, status.Error(codes.InvalidArgument, "document is required")
	}
	if doc.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "document.name is required")
	}

	// Determine write mode from precondition.
	writeMode := store.WriteModeUpsert
	if pre := req.GetCurrentDocument(); pre != nil {
		switch c := pre.GetConditionType().(type) {
		case *firestorev1.Precondition_Exists:
			if c.Exists {
				writeMode = store.WriteModeUpdate
			} else {
				writeMode = store.WriteModeInsertOnly
			}
		case *firestorev1.Precondition_UpdateTime:
			// Plan 2: treat update_time precondition as "must exist".
			// Full optimistic locking (version matching) is added in Plan 3.
			writeMode = store.WriteModeUpdate
		}
	}

	// Apply field mask (read-modify-write) if set.
	fields := doc.GetFields()
	if mask := req.GetUpdateMask(); mask != nil && len(mask.GetFieldPaths()) > 0 {
		curr, err := s.adapter(ctx).GetDocument(ctx, doc.GetName())
		if err != nil {
			if status.Code(err) == codes.NotFound &&
				(writeMode == store.WriteModeUpsert || writeMode == store.WriteModeInsertOnly) {
				// Document absent; use the provided fields as-is.
				// For WriteModeUpsert: create it. For WriteModeInsertOnly: create it (precondition satisfied).
				//
				// NOTE: WriteModeUpsert retains upsert semantics here, not InsertOnly. If another writer
				// creates the document between this GetDocument and the UpdateDocument call below, the upsert
				// will silently overwrite it. Plan 3 will add optimistic locking to close this race window.
			} else {
				return nil, err
			}
		} else {
			// Document exists. InsertOnly precondition is violated — caller demanded non-existence.
			if writeMode == store.WriteModeInsertOnly {
				return nil, status.Errorf(codes.AlreadyExists, "document already exists: %s", doc.GetName())
			}
			currProto, err := codec.StoreToProto(curr)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "decode current document: %v", err)
			}
			fields = applyMask(currProto.GetFields(), doc.GetFields(), mask.GetFieldPaths())
			writeMode = store.WriteModeUpdate // doc existed; switch to update-only
		}
	}

	writeProto := &firestorev1.Document{Name: doc.GetName(), Fields: fields}
	sd, err := codec.ProtoToStore(writeProto)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode document: %v", err)
	}

	result, err := s.adapter(ctx).UpdateDocument(ctx, sd, writeMode)
	if err != nil {
		return nil, err
	}
	return codec.StoreToProto(result)
}

// DeleteDocument removes a document.
func (s *firestoreServer) DeleteDocument(ctx context.Context, req *firestorev1.DeleteDocumentRequest) (*emptypb.Empty, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	mustExist := false
	if pre := req.GetCurrentDocument(); pre != nil {
		switch c := pre.GetConditionType().(type) {
		case *firestorev1.Precondition_Exists:
			mustExist = c.Exists
		case *firestorev1.Precondition_UpdateTime:
			mustExist = true
		}
	}
	if err := s.adapter(ctx).DeleteDocument(ctx, req.GetName(), mustExist); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ListDocuments lists documents in a collection.
func (s *firestoreServer) ListDocuments(ctx context.Context, req *firestorev1.ListDocumentsRequest) (*firestorev1.ListDocumentsResponse, error) {
	if req.GetParent() == "" {
		return nil, status.Error(codes.InvalidArgument, "parent is required")
	}
	page, err := s.adapter(ctx).ListDocuments(ctx,
		req.GetParent(), req.GetCollectionId(),
		req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	docs := make([]*firestorev1.Document, 0, len(page.Documents))
	for _, d := range page.Documents {
		proto, err := codec.StoreToProto(d)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decode document: %v", err)
		}
		docs = append(docs, proto)
	}
	return &firestorev1.ListDocumentsResponse{
		Documents:     docs,
		NextPageToken: page.NextPageToken,
	}, nil
}

// Commit applies a list of writes atomically.
// Supports update (upsert) and delete writes. Field transforms are not yet implemented.
func (s *firestoreServer) Commit(ctx context.Context, req *firestorev1.CommitRequest) (*firestorev1.CommitResponse, error) {
	now := time.Now().UTC()

	// If a transaction ID is provided, delegate to CommitTransaction for OCC.
	if txBytes := req.GetTransaction(); len(txBytes) > 0 {
		txID := string(txBytes)
		type writeKind int
		const (
			kindOp     writeKind = iota // real write op
			kindVerify                  // VerifyMutation (precondition only, no op)
		)
		ops := make([]store.WriteOp, 0, len(req.GetWrites()))
		kinds := make([]writeKind, 0, len(req.GetWrites()))
		for _, w := range req.GetWrites() {
			switch op := w.GetOperation().(type) {
			case *firestorev1.Write_Update:
				// Mask alone does not imply "must exist". updateDoc sends exists:true
				// explicitly; setDoc({merge}) sends a mask with no precondition.
				mode := store.WriteModeUpsert
				sd, err := codec.ProtoToStore(op.Update)
				if err != nil {
					return nil, status.Errorf(codes.Internal, "encode document: %v", err)
				}
				ops = append(ops, store.WriteOp{Type: store.WriteOpUpdate, Doc: sd, Mode: mode})
				kinds = append(kinds, kindOp)
			case *firestorev1.Write_Delete:
				ops = append(ops, store.WriteOp{Type: store.WriteOpDelete, Path: op.Delete})
				kinds = append(kinds, kindOp)
			case nil:
				// VerifyMutation: precondition only, OCC enforced by transaction read set.
				kinds = append(kinds, kindVerify)
			default:
				return nil, status.Error(codes.Unimplemented, "write operation type not supported in transaction commit")
			}
		}
		result, err := s.adapter(ctx).CommitTransaction(ctx, txID, ops)
		if err != nil {
			return nil, err
		}
		// One WriteResult per Write (Firestore spec). VerifyMutation writes get an
		// empty WriteResult with the commit time; real writes get their UpdatedAt.
		commitTs := timestamppb.New(result.CommitTime)
		wrs := make([]*firestorev1.WriteResult, 0, len(kinds))
		opIdx := 0
		for _, k := range kinds {
			if k == kindVerify {
				wrs = append(wrs, &firestorev1.WriteResult{UpdateTime: commitTs})
			} else {
				wrs = append(wrs, &firestorev1.WriteResult{UpdateTime: timestamppb.New(result.WriteResults[opIdx].UpdatedAt)})
				opIdx++
			}
		}
		return &firestorev1.CommitResponse{
			WriteResults: wrs,
			CommitTime:   commitTs,
		}, nil
	}

	var results []*firestorev1.WriteResult
	if err := s.adapter(ctx).WithTransaction(ctx, func(txCtx context.Context) error {
		var batchErr error
		results, batchErr = s.applyWriteBatch(txCtx, req.GetWrites(), now)
		return batchErr
	}); err != nil {
		return nil, err
	}
	return &firestorev1.CommitResponse{
		WriteResults: results,
		CommitTime:   timestamppb.New(now),
	}, nil
}

// applyWriteBatch applies a slice of writes (non-transactional) and returns
// WriteResults. It is shared by Commit (non-transaction path) and Write stream.
func (s *firestoreServer) applyWriteBatch(ctx context.Context, writes []*firestorev1.Write, now time.Time) ([]*firestorev1.WriteResult, error) {
	results := make([]*firestorev1.WriteResult, 0, len(writes))

	for _, w := range writes {
		switch op := w.GetOperation().(type) {
		case *firestorev1.Write_Update:
			doc := op.Update
			if doc.GetName() == "" {
				return nil, status.Error(codes.InvalidArgument, "write.update.name is required")
			}
			mask := w.GetUpdateMask()
			hasMask := mask != nil && len(mask.GetFieldPaths()) > 0
			// Default is upsert (create or overwrite). A mask alone does NOT imply
			// "must exist" — setDoc({merge:true}) sends a mask with no precondition.
			// Only an explicit currentDocument.exists=true precondition switches to
			// update-only mode.
			mode := store.WriteModeUpsert
			// Apply currentDocument precondition for the update operation.
			var preconditionUpdateTime *timestamppb.Timestamp
			if pre := w.GetCurrentDocument(); pre != nil {
				switch c := pre.GetConditionType().(type) {
				case *firestorev1.Precondition_Exists:
					if c.Exists {
						mode = store.WriteModeUpdate
					} else {
						mode = store.WriteModeInsertOnly
					}
				case *firestorev1.Precondition_UpdateTime:
					preconditionUpdateTime = c.UpdateTime
					mode = store.WriteModeUpdate
				}
			}

			// Determine if we need to read the current document.
			// Required when: mask is present, transforms present, or
			// updateTime precondition must be validated.
			transforms := w.GetUpdateTransforms()
			needsRead := hasMask || preconditionUpdateTime != nil || len(transforms) > 0
			var currFields map[string]*firestorev1.Value
			if needsRead {
				curr, err := s.adapter(ctx).GetDocument(ctx, doc.GetName())
				if err == nil {
					currDoc, _ := codec.StoreToProto(curr)
					currFields = currDoc.GetFields()
					// Validate updateTime precondition.
					if preconditionUpdateTime != nil {
						if !curr.UpdatedAt.Truncate(time.Microsecond).Equal(preconditionUpdateTime.AsTime().Truncate(time.Microsecond)) {
							return nil, status.Errorf(codes.FailedPrecondition,
								"document %s has been modified: expected updateTime %v, got %v",
								doc.GetName(), preconditionUpdateTime.AsTime(), curr.UpdatedAt)
						}
					}
				} else if preconditionUpdateTime != nil {
					// updateTime precondition on a missing document always fails.
					return nil, status.Errorf(codes.FailedPrecondition,
						"document %s does not exist, cannot satisfy updateTime precondition", doc.GetName())
				}
			}

			// Apply field transforms.
			fields := doc.GetFields()
			var transformResults []*firestorev1.Value
			if len(transforms) > 0 {
				// Transform-only writes (no mask, no explicit fields) are deltas on the
				// current document. Use currFields as the base so that sibling fields are
				// preserved — e.g. updateDoc({meta.updatedAt: serverTimestamp()}) must
				// keep meta.name intact.
				baseFields := fields
				if !hasMask && len(fields) == 0 && currFields != nil {
					baseFields = currFields
				}
				var transformErr error
				fields, transformResults, transformErr = applyFieldTransforms(baseFields, transforms, currFields, now)
				if transformErr != nil {
					return nil, transformErr
				}
			}

			// When update_mask is present, merge only the masked fields into the
			// existing document. Nested dot-notation paths (e.g. "a.b.c") are
			// handled recursively; siblings are preserved.
			if hasMask {
				fields = applyMask(currFields, fields, mask.GetFieldPaths())
			}

			writeDoc := &firestorev1.Document{Name: doc.GetName(), Fields: fields}
			sd, err := codec.ProtoToStore(writeDoc)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "encode document: %v", err)
			}
			result, err := s.adapter(ctx).UpdateDocument(ctx, sd, mode)
			if err != nil {
				return nil, err
			}
			results = append(results, &firestorev1.WriteResult{
				UpdateTime:       timestamppb.New(result.UpdatedAt),
				TransformResults: transformResults,
			})

		case *firestorev1.Write_Delete:
			if op.Delete == "" {
				return nil, status.Error(codes.InvalidArgument, "write.delete path is required")
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
			if err := s.adapter(ctx).DeleteDocument(ctx, op.Delete, mustExist); err != nil {
				return nil, err
			}
			results = append(results, &firestorev1.WriteResult{
				UpdateTime: timestamppb.New(now),
			})

		case nil:
			// VerifyMutation: no operation, only a currentDocument precondition.
			// In this proto's generated code there's no path-bearing `verify`
			// field, so the precondition can't be checked against a target —
			// but we MUST still append a WriteResult to keep the per-write
			// length contract that the Firebase SDK enforces.
			results = append(results, &firestorev1.WriteResult{
				UpdateTime: timestamppb.New(now),
			})

		default:
			return nil, status.Error(codes.Unimplemented, "write operation type not yet supported")
		}
	}
	return results, nil
}

// Write implements the bidirectional Write stream used by the Firebase full SDK
// for all mutation operations (addDoc, setDoc, updateDoc, deleteDoc, writeBatch).
//
// Protocol:
//  1. Client sends handshake WriteRequest (empty writes, stream_id="").
//  2. Server sends WriteResponse with stream_id + stream_token (no write results).
//  3. Client sends WriteRequest batches; server applies writes and returns WriteResults.
func (s *firestoreServer) Write(stream firestorev1.Firestore_WriteServer) error {
	// Step 1: consume the handshake.
	if _, err := stream.Recv(); err != nil {
		return err
	}

	// Step 2: send handshake response.
	streamID := fmt.Sprintf("%016x", time.Now().UnixNano())
	now := time.Now().UTC()
	token := []byte(now.Format(time.RFC3339Nano))
	if err := stream.Send(&firestorev1.WriteResponse{
		StreamId:    streamID,
		StreamToken: token,
		CommitTime:  timestamppb.New(now),
	}); err != nil {
		return err
	}

	// Step 3: write loop.
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			s.log.Info("Write stream EOF")
			return nil
		}
		if err != nil {
			if status.Code(err) == codes.Canceled {
				s.log.Info("Write stream canceled")
				return nil
			}
			s.log.Error("Write stream Recv error", zap.Error(err))
			return err
		}

		s.log.Info("Write stream: applying batch", zap.Int("writes", len(req.GetWrites())))
		now = time.Now().UTC()
		ctx := stream.Context()
		var results []*firestorev1.WriteResult
		if err := s.adapter(ctx).WithTransaction(ctx, func(txCtx context.Context) error {
			var batchErr error
			results, batchErr = s.applyWriteBatch(txCtx, req.GetWrites(), now)
			return batchErr
		}); err != nil {
			s.log.Error("Write stream: applyWriteBatch error", zap.Error(err))
			return err
		}
		token = []byte(now.Format(time.RFC3339Nano))
		if err := stream.Send(&firestorev1.WriteResponse{
			StreamId:     streamID,
			StreamToken:  token,
			WriteResults: results,
			CommitTime:   timestamppb.New(now),
		}); err != nil {
			return err
		}
	}
}

// BatchGetDocuments fetches multiple documents in a single streaming RPC.
// The Firebase SDK uses this to read documents inside runTransaction.
// Supports three consistency modes:
//   - existing transaction ID  → reads participate in that transaction
//   - new_transaction          → server starts a transaction and returns its ID
//   - no selector              → snapshot read outside any transaction
func (s *firestoreServer) BatchGetDocuments(req *firestorev1.BatchGetDocumentsRequest, stream firestorev1.Firestore_BatchGetDocumentsServer) error {
	ctx := stream.Context()

	// Resolve the transaction ID to use for reads.
	txID := ""
	ownedTx := false // true when this handler started the transaction
	switch cs := req.GetConsistencySelector().(type) {
	case *firestorev1.BatchGetDocumentsRequest_Transaction:
		txID = string(cs.Transaction)

	case *firestorev1.BatchGetDocumentsRequest_NewTransaction:
		readOnly := false
		if ro := cs.NewTransaction.GetMode(); ro != nil {
			_, readOnly = ro.(*firestorev1.TransactionOptions_ReadOnly_)
		}
		var err error
		txID, err = s.adapter(ctx).BeginTransaction(ctx, readOnly)
		if err != nil {
			return err
		}
		ownedTx = true
		// Roll back if this handler fails; cleared on success so client can Commit/Rollback.
		defer func() {
			if ownedTx {
				_ = s.adapter(ctx).RollbackTransaction(ctx, txID)
			}
		}()
		// First response carries the new transaction ID so the client can later Commit/Rollback.
		if err := stream.Send(&firestorev1.BatchGetDocumentsResponse{
			Transaction: []byte(txID),
			ReadTime:    timestamppb.Now(),
		}); err != nil {
			return err
		}
	}

	readTime := timestamppb.Now()
	for _, path := range req.GetDocuments() {
		var resp *firestorev1.BatchGetDocumentsResponse

		var sd *store.Document
		var err error
		if txID != "" {
			sd, err = s.adapter(ctx).GetDocumentForTransaction(ctx, txID, path)
		} else {
			sd, err = s.adapter(ctx).GetDocument(ctx, path)
		}

		if err != nil {
			if status.Code(err) == codes.NotFound {
				resp = &firestorev1.BatchGetDocumentsResponse{
					Result:   &firestorev1.BatchGetDocumentsResponse_Missing{Missing: path},
					ReadTime: readTime,
				}
			} else {
				return err
			}
		} else {
			proto, err := codec.StoreToProto(sd)
			if err != nil {
				return status.Errorf(codes.Internal, "decode document: %v", err)
			}
			resp = &firestorev1.BatchGetDocumentsResponse{
				Result:   &firestorev1.BatchGetDocumentsResponse_Found{Found: proto},
				ReadTime: readTime,
			}
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	ownedTx = false // success: client is responsible for Commit/Rollback
	return nil
}

// batchGetStreamer implements Firestore_BatchGetDocumentsServer by collecting
// all responses into a JSON array, matching the real Firestore REST behaviour.
// The Firebase SDK calls i.forEach() on the result, so the whole array must be
// returned at once — unlike RunQuery, BatchGetDocuments is not streamed incrementally.
type batchGetStreamer struct {
	ctx     context.Context
	buf     []*firestorev1.BatchGetDocumentsResponse
	marshal protojson.MarshalOptions
}

func (s *batchGetStreamer) Send(r *firestorev1.BatchGetDocumentsResponse) error {
	s.buf = append(s.buf, r)
	return nil
}
func (s *batchGetStreamer) SetHeader(md metadata.MD) error  { return nil }
func (s *batchGetStreamer) SendHeader(md metadata.MD) error { return nil }
func (s *batchGetStreamer) SetTrailer(metadata.MD)          {}
func (s *batchGetStreamer) Context() context.Context        { return s.ctx }
func (s *batchGetStreamer) SendMsg(m any) error             { return nil }
func (s *batchGetStreamer) RecvMsg(m any) error             { return nil }

// serveBatchGetDocuments intercepts POST …:batchGet and returns a JSON array.
// grpc-gateway emits NDJSON for server-streaming RPCs, but the Firebase SDK
// calls .forEach() on the full response body, so it must be a JSON array.
func serveBatchGetDocuments(w http.ResponseWriter, r *http.Request, fs *firestoreServer) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// URL: /v1/projects/p/databases/(default)/documents:batchGet
	// Extract database = "projects/p/databases/(default)"
	stripped := strings.TrimPrefix(r.URL.Path, "/v1/")
	database := strings.TrimSuffix(stripped, "/documents:batchGet")

	req := &firestorev1.BatchGetDocumentsRequest{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(body, req); err != nil {
		http.Error(w, "decode request: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.Database = database

	streamer := &batchGetStreamer{
		ctx:     r.Context(),
		marshal: protojson.MarshalOptions{EmitUnpopulated: false},
	}

	if rpcErr := fs.BatchGetDocuments(req, streamer); rpcErr != nil {
		code := status.Code(rpcErr)
		http.Error(w, rpcErr.Error(), grpcCodeToHTTP(code))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("["))
	for i, resp := range streamer.buf {
		if i > 0 {
			w.Write([]byte(","))
		}
		b, _ := streamer.marshal.Marshal(resp)
		w.Write(b)
	}
	w.Write([]byte("]"))
}

// RunQuery executes a structured query against a collection.
// Supports filters, ordering, and pagination via codec.QueryFromStructuredQuery
// and s.db.QueryDocuments.
func (s *firestoreServer) RunQuery(req *firestorev1.RunQueryRequest, stream firestorev1.Firestore_RunQueryServer) error {
	ctx := stream.Context()

	sq := req.GetStructuredQuery()
	if sq == nil {
		return status.Error(codes.InvalidArgument, "structured_query is required")
	}
	froms := sq.GetFrom()
	if len(froms) == 0 {
		return status.Error(codes.InvalidArgument, "structured_query.from is required")
	}

	parent := req.GetParent()
	pageSize := int32(300)
	if lim := sq.GetLimit(); lim != nil && lim.GetValue() > 0 {
		pageSize = lim.GetValue()
	}

	q, err := codec.QueryFromStructuredQuery(parent, sq, pageSize, "")
	if err != nil {
		return err
	}

	readTime := timestamppb.Now()
	pageToken := ""
	for {
		q.PageToken = pageToken
		page, err := s.adapter(ctx).QueryDocuments(ctx, q)
		if err != nil {
			return err
		}
		for _, sd := range page.Documents {
			proto, err := codec.StoreToProto(sd)
			if err != nil {
				return status.Errorf(codes.Internal, "decode document: %v", err)
			}
			if err := stream.Send(&firestorev1.RunQueryResponse{
				Document: proto,
				ReadTime: readTime,
			}); err != nil {
				return err
			}
		}
		if page.NextPageToken == "" || q.Limit > 0 {
			break
		}
		pageToken = page.NextPageToken
	}

	return stream.Send(&firestorev1.RunQueryResponse{
		ContinuationSelector: &firestorev1.RunQueryResponse_Done{Done: true},
		ReadTime:             readTime,
	})
}

// runQueryStreamer implements Firestore_RunQueryServer by writing each response
// directly to an http.ResponseWriter as a streaming JSON array, matching real
// Firestore REST behaviour: `[` is written before the first item, `,` is
// inserted between items, and the caller closes the array with `]`.
// http.Flusher.Flush() is called after each item so bytes reach the client
// as soon as each document is ready rather than at the end of the query.
type runQueryStreamer struct {
	ctx     context.Context
	w       http.ResponseWriter
	flusher http.Flusher
	marshal protojson.MarshalOptions
	started bool // true once the opening `[` has been written
}

func (s *runQueryStreamer) Send(r *firestorev1.RunQueryResponse) error {
	if !s.started {
		s.w.Write([]byte("["))
		s.started = true
	} else {
		s.w.Write([]byte(","))
	}
	b, _ := s.marshal.Marshal(r)
	s.w.Write(b)
	if s.flusher != nil {
		s.flusher.Flush()
	}
	return nil
}
func (s *runQueryStreamer) SetHeader(md metadata.MD) error  { return nil }
func (s *runQueryStreamer) SendHeader(md metadata.MD) error { return nil }
func (s *runQueryStreamer) SetTrailer(metadata.MD)          {}
func (s *runQueryStreamer) Context() context.Context        { return s.ctx }
func (s *runQueryStreamer) SendMsg(m any) error             { return nil }
func (s *runQueryStreamer) RecvMsg(m any) error             { return nil }

// applyMask merges incoming fields into current fields honouring the field mask.
// Mask paths may use dot-notation for nested fields (e.g. "profile.age").
// Fields in maskPaths are replaced by the corresponding incoming value, or deleted
// if absent from incoming. Fields not in maskPaths are kept unchanged.
// Intermediate map nodes are deep-copied before modification to avoid aliasing.
func applyMask(current, incoming map[string]*firestorev1.Value, maskPaths []string) map[string]*firestorev1.Value {
	result := cloneFields(current)
	for _, fp := range maskPaths {
		segments := strings.SplitN(fp, ".", 2)
		top := segments[0]
		if len(segments) == 1 {
			// Leaf at the top level.
			if v, ok := incoming[top]; ok {
				result[top] = v
			} else {
				delete(result, top)
			}
		} else {
			// Nested path: recurse into the map at top.
			rest := segments[1]
			incomingChild := getNestedFields(incoming, top)
			currentChild := getNestedFields(result, top)
			merged := applyMask(currentChild, incomingChild, []string{rest})
			if result[top] == nil {
				result[top] = &firestorev1.Value{ValueType: &firestorev1.Value_MapValue{
					MapValue: &firestorev1.MapValue{Fields: merged},
				}}
			} else {
				// Clone the Value node so we don't alias the stored proto.
				result[top] = &firestorev1.Value{ValueType: &firestorev1.Value_MapValue{
					MapValue: &firestorev1.MapValue{Fields: merged},
				}}
			}
		}
	}
	return result
}

// cloneFields returns a shallow copy of the fields map (new map, same value pointers).
func cloneFields(m map[string]*firestorev1.Value) map[string]*firestorev1.Value {
	c := make(map[string]*firestorev1.Value, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

// getNestedFields returns the Fields map inside a MapValue at key top, or nil.
func getNestedFields(m map[string]*firestorev1.Value, top string) map[string]*firestorev1.Value {
	v, ok := m[top]
	if !ok || v == nil {
		return nil
	}
	mv := v.GetMapValue()
	if mv == nil {
		return nil
	}
	return mv.GetFields()
}

// nestedGetValue traverses a dot-notation field path and returns the leaf value.
func nestedGetValue(fields map[string]*firestorev1.Value, fp string) *firestorev1.Value {
	segments := strings.SplitN(fp, ".", 2)
	if len(segments) == 1 {
		return fields[segments[0]]
	}
	child := getNestedFields(fields, segments[0])
	if child == nil {
		return nil
	}
	return nestedGetValue(child, segments[1])
}

// nestedSetValue sets a value at a dot-notation field path, creating map nodes as needed.
func nestedSetValue(result map[string]*firestorev1.Value, fp string, val *firestorev1.Value) {
	segments := strings.SplitN(fp, ".", 2)
	top := segments[0]
	if len(segments) == 1 {
		result[top] = val
		return
	}
	child := getNestedFields(result, top)
	childCopy := make(map[string]*firestorev1.Value, len(child))
	for k, v := range child {
		childCopy[k] = v
	}
	nestedSetValue(childCopy, segments[1], val)
	result[top] = &firestorev1.Value{ValueType: &firestorev1.Value_MapValue{
		MapValue: &firestorev1.MapValue{Fields: childCopy},
	}}
}

// applyFieldTransforms applies field transforms and returns the updated fields
// map plus one transform result per transform entry (required by WriteResult).
func applyFieldTransforms(
	fields map[string]*firestorev1.Value,
	transforms []*firestorev1.DocumentTransform_FieldTransform,
	currFields map[string]*firestorev1.Value,
	now time.Time,
) (map[string]*firestorev1.Value, []*firestorev1.Value, error) {
	if len(transforms) == 0 {
		return fields, nil, nil
	}
	// Copy so we don't mutate the input proto.
	result := make(map[string]*firestorev1.Value, len(fields))
	for k, v := range fields {
		result[k] = v
	}
	transformResults := make([]*firestorev1.Value, 0, len(transforms))

	for _, t := range transforms {
		fp := t.GetFieldPath()

		switch tt := t.GetTransformType().(type) {

		case *firestorev1.DocumentTransform_FieldTransform_SetToServerValue:
			var val *firestorev1.Value
			if tt.SetToServerValue == firestorev1.DocumentTransform_FieldTransform_REQUEST_TIME {
				val = &firestorev1.Value{
					ValueType: &firestorev1.Value_TimestampValue{
						TimestampValue: timestamppb.New(now),
					},
				}
				nestedSetValue(result, fp, val)
			}
			transformResults = append(transformResults, val)

		case *firestorev1.DocumentTransform_FieldTransform_Increment:
			delta := tt.Increment
			curr := nestedGetValue(currFields, fp)
			var val *firestorev1.Value
			switch d := delta.GetValueType().(type) {
			case *firestorev1.Value_IntegerValue:
				existing := int64(0)
				if curr != nil {
					if iv, ok := curr.GetValueType().(*firestorev1.Value_IntegerValue); ok {
						existing = iv.IntegerValue
					}
				}
				val = &firestorev1.Value{ValueType: &firestorev1.Value_IntegerValue{
					IntegerValue: existing + d.IntegerValue,
				}}
			case *firestorev1.Value_DoubleValue:
				existing := float64(0)
				if curr != nil {
					switch ev := curr.GetValueType().(type) {
					case *firestorev1.Value_DoubleValue:
						existing = ev.DoubleValue
					case *firestorev1.Value_IntegerValue:
						existing = float64(ev.IntegerValue)
					}
				}
				val = &firestorev1.Value{ValueType: &firestorev1.Value_DoubleValue{
					DoubleValue: existing + d.DoubleValue,
				}}
			default:
				return nil, nil, status.Errorf(codes.InvalidArgument,
					"increment delta must be integer or double, got %T", delta.GetValueType())
			}
			nestedSetValue(result, fp, val)
			transformResults = append(transformResults, val)

		case *firestorev1.DocumentTransform_FieldTransform_AppendMissingElements:
			curr := nestedGetValue(currFields, fp)
			var existing []*firestorev1.Value
			if curr != nil {
				if av, ok := curr.GetValueType().(*firestorev1.Value_ArrayValue); ok {
					existing = av.ArrayValue.GetValues()
				}
			}
			incoming := tt.AppendMissingElements.GetValues()
			merged := append([]*firestorev1.Value(nil), existing...)
			for _, v := range incoming {
				found := false
				for _, e := range existing {
					if proto.Equal(e, v) {
						found = true
						break
					}
				}
				if !found {
					merged = append(merged, v)
				}
			}
			mergedVal := &firestorev1.Value{ValueType: &firestorev1.Value_ArrayValue{
				ArrayValue: &firestorev1.ArrayValue{Values: merged},
			}}
			nestedSetValue(result, fp, mergedVal)
			transformResults = append(transformResults, mergedVal)

		case *firestorev1.DocumentTransform_FieldTransform_RemoveAllFromArray:
			curr := nestedGetValue(currFields, fp)
			emptyArr := &firestorev1.Value{ValueType: &firestorev1.Value_ArrayValue{
				ArrayValue: &firestorev1.ArrayValue{},
			}}
			if curr == nil {
				// Field absent — arrayRemove is a no-op; don't create the field.
				transformResults = append(transformResults, emptyArr)
				continue
			}
			var existing []*firestorev1.Value
			if av, ok := curr.GetValueType().(*firestorev1.Value_ArrayValue); ok {
				existing = av.ArrayValue.GetValues()
			}
			toRemove := tt.RemoveAllFromArray.GetValues()
			kept := existing[:0:0]
			for _, e := range existing {
				remove := false
				for _, r := range toRemove {
					if proto.Equal(e, r) {
						remove = true
						break
					}
				}
				if !remove {
					kept = append(kept, e)
				}
			}
			keptVal := &firestorev1.Value{ValueType: &firestorev1.Value_ArrayValue{
				ArrayValue: &firestorev1.ArrayValue{Values: kept},
			}}
			nestedSetValue(result, fp, keptVal)
			transformResults = append(transformResults, keptVal)
		}
	}
	return result, transformResults, nil
}
