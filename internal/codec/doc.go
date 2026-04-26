package codec

import (
	"crypto/rand"
	"fmt"
	"io"
	"time"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"github.com/petereon/firstyr/internal/store"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// documentIDChars is the alphabet for generated document IDs.
const documentIDChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// NewDocumentID generates a 20-character random alphanumeric Firestore-style document ID.
// Uses rejection sampling to avoid modulo bias: bytes >= 248 are discarded.
func NewDocumentID() (string, error) {
	id := make([]byte, 0, 20)
	buf := make([]byte, 32)
	for len(id) < 20 {
		if _, err := io.ReadFull(rand.Reader, buf); err != nil {
			return "", fmt.Errorf("codec: read random bytes: %w", err)
		}
		for _, v := range buf {
			if v < 248 { // reject values that would bias chars 0-7
				id = append(id, documentIDChars[v%62])
				if len(id) == 20 {
					break
				}
			}
		}
	}
	return string(id), nil
}

// ParsePath extracts the immediate collection name and parent path from a full
// Firestore document path. Delegates to store.ParsePath — single canonical implementation.
func ParsePath(path string) (collection, parent string) {
	return store.ParsePath(path)
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

	now := time.Now().UTC()
	createdAt := now
	if ct := doc.GetCreateTime(); ct != nil && ct.IsValid() {
		createdAt = ct.AsTime().UTC()
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
