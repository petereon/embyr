package server

import (
	"context"
	"strings"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"github.com/petereon/firstyr/internal/codec"
	"github.com/petereon/firstyr/internal/store"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
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
	path := codec.BuildPath(req.GetParent(), req.GetCollectionId(), docID)

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
			if status.Code(err) == codes.NotFound && writeMode == store.WriteModeUpsert {
				// Document doesn't exist yet; use the provided fields as-is.
			} else {
				return nil, err
			}
		} else {
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

// applyMask merges incoming fields into current fields, honouring the field mask.
// Fields listed in maskPaths are replaced by the incoming value (or removed if
// absent in incoming). Fields not in maskPaths are kept from current.
// Only top-level field paths are supported in Plan 2.
func applyMask(current, incoming map[string]*firestorev1.Value, maskPaths []string) map[string]*firestorev1.Value {
	result := make(map[string]*firestorev1.Value, len(current))
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
