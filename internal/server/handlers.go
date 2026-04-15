package server

import (
	"context"
	"net/http"
	"strings"
	"time"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"github.com/petereon/firstyr/internal/codec"
	"github.com/petereon/firstyr/internal/store"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// firestoreServer is the gRPC service implementation.
type firestoreServer struct {
	firestorev1.UnimplementedFirestoreServer
	db  store.StorageAdapter
	log *zap.Logger
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
	results := make([]*firestorev1.WriteResult, 0, len(req.GetWrites()))

	for _, w := range req.GetWrites() {
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
			sd, err := codec.ProtoToStore(doc)
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

		default:
			return nil, status.Error(codes.Unimplemented, "write operation type not yet supported")
		}
	}

	return &firestorev1.CommitResponse{
		WriteResults: results,
		CommitTime:   timestamppb.New(now),
	}, nil
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
