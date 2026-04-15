package codec

import (
	"crypto/rand"
	"fmt"
	"io"
	"strings"
	"time"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"github.com/petereon/firstyr/internal/store"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// documentIDChars is the alphabet for generated document IDs.
const documentIDChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// NewDocumentID generates a 20-character random Firestore-style document ID.
func NewDocumentID() string {
	b := make([]byte, 20)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		panic(fmt.Sprintf("codec: read random bytes: %v", err))
	}
	for i := range b {
		b[i] = documentIDChars[b[i]%62]
	}
	return string(b)
}

// ParsePath extracts the immediate collection name and parent path from a full
// Firestore document path.
//
// Example:
//
//	ParsePath("projects/p/databases/d/documents/users/alice")
//	→ collection="users", parent="projects/p/databases/d/documents"
//
//	ParsePath("projects/p/databases/d/documents/users/alice/orders/123")
//	→ collection="orders", parent="projects/p/databases/d/documents/users/alice"
func ParsePath(path string) (collection, parent string) {
	parts := strings.Split(path, "/")
	docsIdx := -1
	for i, p := range parts {
		if p == "documents" {
			docsIdx = i
			break
		}
	}
	if docsIdx < 0 {
		return "", ""
	}
	relative := parts[docsIdx+1:]
	if len(relative) < 2 {
		return "", ""
	}
	collection = relative[len(relative)-2]
	parentParts := make([]string, 0, docsIdx+1+len(relative)-2)
	parentParts = append(parentParts, parts[:docsIdx+1]...)
	parentParts = append(parentParts, relative[:len(relative)-2]...)
	parent = strings.Join(parentParts, "/")
	return collection, parent
}

// BuildPath constructs a full Firestore document path from its components.
func BuildPath(parent, collectionID, docID string) string {
	return parent + "/" + collectionID + "/" + docID
}

// marshaler marshals only the fields, omitting name/timestamps.
var marshaler = protojson.MarshalOptions{EmitUnpopulated: false}

// unmarshaler discards unknown fields (name, create_time, etc. from older data).
var unmarshaler = protojson.UnmarshalOptions{DiscardUnknown: true}

// ProtoToStore converts a proto Document into the backend-neutral store.Document.
// The Data field is set to the protojson representation of the document's fields.
// CreatedAt defaults to now if not set in proto; UpdatedAt is always set to now.
func ProtoToStore(doc *firestorev1.Document) (*store.Document, error) {
	// Marshal only the Fields map by encoding a fields-only Document.
	tmp := &firestorev1.Document{Fields: doc.GetFields()}
	b, err := marshaler.Marshal(tmp)
	if err != nil {
		return nil, fmt.Errorf("codec: marshal fields: %w", err)
	}
	data := string(b)
	if data == "" {
		data = "{}"
	}

	now := time.Now().UTC()
	createdAt := now
	if doc.GetCreateTime() != nil {
		createdAt = doc.GetCreateTime().AsTime().UTC()
	}

	return &store.Document{
		Path:      doc.GetName(),
		Data:      data,
		CreatedAt: createdAt,
		UpdatedAt: now,
		Version:   1,
	}, nil
}

// StoreToProto converts a store.Document back into a proto Document.
func StoreToProto(d *store.Document) (*firestorev1.Document, error) {
	tmp := &firestorev1.Document{}
	if d.Data != "" && d.Data != "{}" {
		if err := unmarshaler.Unmarshal([]byte(d.Data), tmp); err != nil {
			return nil, fmt.Errorf("codec: unmarshal fields: %w", err)
		}
	}
	return &firestorev1.Document{
		Name:       d.Path,
		Fields:     tmp.Fields,
		CreateTime: timestamppb.New(d.CreatedAt),
		UpdateTime: timestamppb.New(d.UpdatedAt),
	}, nil
}
