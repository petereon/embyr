package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"github.com/petereon/firstyr/internal/codec"
	"github.com/petereon/firstyr/internal/listen"
	"github.com/petereon/firstyr/internal/store"
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

// GetDocument fetches a single document by its resource name.
func (s *firestoreServer) GetDocument(ctx context.Context, req *firestorev1.GetDocumentRequest) (*firestorev1.Document, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	d, err := s.db.GetDocument(ctx, req.GetName())
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
		docID = codec.NewDocumentID()
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
	created, err := s.db.CreateDocument(ctx, sd)
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
		curr, err := s.db.GetDocument(ctx, doc.GetName())
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

	result, err := s.db.UpdateDocument(ctx, sd, writeMode)
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
	if err := s.db.DeleteDocument(ctx, req.GetName(), mustExist); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ListDocuments lists documents in a collection.
func (s *firestoreServer) ListDocuments(ctx context.Context, req *firestorev1.ListDocumentsRequest) (*firestorev1.ListDocumentsResponse, error) {
	if req.GetParent() == "" {
		return nil, status.Error(codes.InvalidArgument, "parent is required")
	}
	page, err := s.db.ListDocuments(ctx,
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
		ops := make([]store.WriteOp, 0, len(req.GetWrites()))
		for _, w := range req.GetWrites() {
			switch op := w.GetOperation().(type) {
			case *firestorev1.Write_Update:
				mode := store.WriteModeUpsert
				if mask := w.GetUpdateMask(); mask != nil && len(mask.GetFieldPaths()) > 0 {
					mode = store.WriteModeUpdate
				}
				sd, err := codec.ProtoToStore(op.Update)
				if err != nil {
					return nil, status.Errorf(codes.Internal, "encode document: %v", err)
				}
				ops = append(ops, store.WriteOp{Type: store.WriteOpUpdate, Doc: sd, Mode: mode})
			case *firestorev1.Write_Delete:
				ops = append(ops, store.WriteOp{Type: store.WriteOpDelete, Path: op.Delete})
			case nil:
				// VerifyMutation: no operation, only a currentDocument precondition.
				// OCC is enforced by CommitTransaction via the transaction's read set.
				// Nothing to add to ops — just skip.
			default:
				return nil, status.Error(codes.Unimplemented, "write operation type not supported in transaction commit")
			}
		}
		result, err := s.db.CommitTransaction(ctx, txID, ops)
		if err != nil {
			return nil, err
		}
		wrs := make([]*firestorev1.WriteResult, len(result.WriteResults))
		for i, wr := range result.WriteResults {
			wrs[i] = &firestorev1.WriteResult{UpdateTime: timestamppb.New(wr.UpdatedAt)}
		}
		return &firestorev1.CommitResponse{
			WriteResults: wrs,
			CommitTime:   timestamppb.New(result.CommitTime),
		}, nil
	}

	results, err := s.applyWriteBatch(ctx, req.GetWrites(), now)
	if err != nil {
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
			mode := store.WriteModeUpsert
			if mask := w.GetUpdateMask(); mask != nil && len(mask.GetFieldPaths()) > 0 {
				mode = store.WriteModeUpdate
			}
			// Apply currentDocument precondition for the update operation.
			if pre := w.GetCurrentDocument(); pre != nil {
				switch c := pre.GetConditionType().(type) {
				case *firestorev1.Precondition_Exists:
					if c.Exists {
						mode = store.WriteModeUpdate
					} else {
						mode = store.WriteModeInsertOnly
					}
				case *firestorev1.Precondition_UpdateTime:
					_ = c // treat update_time precondition as "must exist"
					mode = store.WriteModeUpdate
				}
			}

			// Apply field transforms if present.
			transforms := w.GetUpdateTransforms()
			fields := doc.GetFields()
			if len(transforms) > 0 {
				var currFields map[string]*firestorev1.Value
				needsRead := false
				for _, t := range transforms {
					switch t.GetTransformType().(type) {
					case *firestorev1.DocumentTransform_FieldTransform_Increment,
						*firestorev1.DocumentTransform_FieldTransform_AppendMissingElements,
						*firestorev1.DocumentTransform_FieldTransform_RemoveAllFromArray:
						needsRead = true
					}
				}
				if needsRead {
					curr, err := s.db.GetDocument(ctx, doc.GetName())
					if err == nil {
						currDoc, _ := codec.StoreToProto(curr)
						currFields = currDoc.GetFields()
					}
				}
				var transformErr error
				fields, transformErr = applyFieldTransforms(fields, transforms, currFields, now)
				if transformErr != nil {
					return nil, transformErr
				}
			}

			writeDoc := &firestorev1.Document{Name: doc.GetName(), Fields: fields}
			sd, err := codec.ProtoToStore(writeDoc)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "encode document: %v", err)
			}
			result, err := s.db.UpdateDocument(ctx, sd, mode)
			if err != nil {
				return nil, err
			}
			results = append(results, &firestorev1.WriteResult{
				UpdateTime: timestamppb.New(result.UpdatedAt),
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
			if err := s.db.DeleteDocument(ctx, op.Delete, mustExist); err != nil {
				return nil, err
			}
			results = append(results, &firestorev1.WriteResult{
				UpdateTime: timestamppb.New(now),
			})

		case nil:
			// VerifyMutation: no operation, only a currentDocument precondition.
			// In the non-transaction path this is a no-op; skip silently.

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
			return nil
		}
		if err != nil {
			if status.Code(err) == codes.Canceled {
				return nil
			}
			return err
		}

		now = time.Now().UTC()
		results, err := s.applyWriteBatch(stream.Context(), req.GetWrites(), now)
		if err != nil {
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
	switch cs := req.GetConsistencySelector().(type) {
	case *firestorev1.BatchGetDocumentsRequest_Transaction:
		txID = string(cs.Transaction)

	case *firestorev1.BatchGetDocumentsRequest_NewTransaction:
		readOnly := false
		if ro := cs.NewTransaction.GetMode(); ro != nil {
			_, readOnly = ro.(*firestorev1.TransactionOptions_ReadOnly_)
		}
		var err error
		txID, err = s.db.BeginTransaction(ctx, readOnly)
		if err != nil {
			return err
		}
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
			sd, err = s.db.GetDocumentForTransaction(ctx, txID, path)
		} else {
			sd, err = s.db.GetDocument(ctx, path)
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
		page, err := s.db.QueryDocuments(ctx, q)
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

// applyMask merges incoming fields into current fields, honouring the field mask.
// Fields listed in maskPaths are replaced by the incoming value (or removed if
// absent in incoming). Fields not in maskPaths are kept from current.
// Only top-level field paths are supported in Plan 2.
func applyMask(current, incoming map[string]*firestorev1.Value, maskPaths []string) map[string]*firestorev1.Value {
	result := make(map[string]*firestorev1.Value, len(current))
	// TODO(plan3): Value pointers from current are copied by reference, not deep-cloned.
	// This is safe because neither the adapter nor codec mutates Value nodes after creation.
	// Plan 3 must deep-clone here if a document cache or proto pooling is introduced —
	// both could cause silent aliasing corruption through this map.
	for k, v := range current {
		result[k] = v
	}
	for _, fp := range maskPaths {
		// Only use the top-level field name (before the first dot).
		topField := fp
		if idx := strings.IndexByte(fp, '.'); idx >= 0 {
			topField = fp[:idx]
		}
		if v, ok := incoming[topField]; ok {
			result[topField] = v
		} else {
			delete(result, topField)
		}
	}
	return result
}

// applyFieldTransforms applies field transforms to fields map in place.
// currFields is the existing document's fields (nil if document doesn't exist yet).
// now is the server timestamp used for REQUEST_TIME transforms.
func applyFieldTransforms(
	fields map[string]*firestorev1.Value,
	transforms []*firestorev1.DocumentTransform_FieldTransform,
	currFields map[string]*firestorev1.Value,
	now time.Time,
) (map[string]*firestorev1.Value, error) {
	if len(transforms) == 0 {
		return fields, nil
	}
	// Copy so we don't mutate the input proto.
	result := make(map[string]*firestorev1.Value, len(fields))
	for k, v := range fields {
		result[k] = v
	}

	for _, t := range transforms {
		fp := t.GetFieldPath()
		// Only top-level field paths supported in plan 3.
		if strings.ContainsRune(fp, '.') {
			return nil, status.Errorf(codes.Unimplemented, "nested field transforms not yet supported: %s", fp)
		}

		switch tt := t.GetTransformType().(type) {

		case *firestorev1.DocumentTransform_FieldTransform_SetToServerValue:
			if tt.SetToServerValue == firestorev1.DocumentTransform_FieldTransform_REQUEST_TIME {
				result[fp] = &firestorev1.Value{
					ValueType: &firestorev1.Value_TimestampValue{
						TimestampValue: timestamppb.New(now),
					},
				}
			}

		case *firestorev1.DocumentTransform_FieldTransform_Increment:
			delta := tt.Increment
			curr := currFields[fp]
			switch d := delta.GetValueType().(type) {
			case *firestorev1.Value_IntegerValue:
				existing := int64(0)
				if curr != nil {
					if iv, ok := curr.GetValueType().(*firestorev1.Value_IntegerValue); ok {
						existing = iv.IntegerValue
					}
				}
				result[fp] = &firestorev1.Value{ValueType: &firestorev1.Value_IntegerValue{
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
				result[fp] = &firestorev1.Value{ValueType: &firestorev1.Value_DoubleValue{
					DoubleValue: existing + d.DoubleValue,
				}}
			}

		case *firestorev1.DocumentTransform_FieldTransform_AppendMissingElements:
			curr := currFields[fp]
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
			result[fp] = &firestorev1.Value{ValueType: &firestorev1.Value_ArrayValue{
				ArrayValue: &firestorev1.ArrayValue{Values: merged},
			}}

		case *firestorev1.DocumentTransform_FieldTransform_RemoveAllFromArray:
			curr := currFields[fp]
			var existing []*firestorev1.Value
			if curr != nil {
				if av, ok := curr.GetValueType().(*firestorev1.Value_ArrayValue); ok {
					existing = av.ArrayValue.GetValues()
				}
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
			result[fp] = &firestorev1.Value{ValueType: &firestorev1.Value_ArrayValue{
				ArrayValue: &firestorev1.ArrayValue{Values: kept},
			}}
		}
	}
	return result, nil
}
