package codec_test

import (
	"testing"
	"time"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"github.com/petereon/firstyr/internal/codec"
	"github.com/petereon/firstyr/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestParsePath(t *testing.T) {
	tests := []struct {
		path       string
		wantColl   string
		wantParent string
	}{
		{
			path:       "projects/p/databases/d/documents/users/alice",
			wantColl:   "users",
			wantParent: "projects/p/databases/d/documents",
		},
		{
			path:       "projects/p/databases/d/documents/users/alice/orders/123",
			wantColl:   "orders",
			wantParent: "projects/p/databases/d/documents/users/alice",
		},
		{
			path:       "projects/p/databases/d/documents/users/alice/orders",
			wantColl:   "",
			wantParent: "",
		},
	}
	for _, tt := range tests {
		coll, parent := codec.ParsePath(tt.path)
		assert.Equal(t, tt.wantColl, coll, "collection for %s", tt.path)
		assert.Equal(t, tt.wantParent, parent, "parent for %s", tt.path)
	}
}

func TestBuildPath(t *testing.T) {
	got := codec.BuildPath("projects/p/databases/d/documents", "users", "alice")
	assert.Equal(t, "projects/p/databases/d/documents/users/alice", got)
}

func TestNewDocumentID(t *testing.T) {
	id := codec.NewDocumentID()
	assert.Len(t, id, 20)
	for _, ch := range id {
		assert.True(t, (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9'),
			"unexpected char %c in document ID", ch)
	}
	// IDs must not collide
	assert.NotEqual(t, id, codec.NewDocumentID())
}

func TestProtoToStoreRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	proto := &firestorev1.Document{
		Name: "projects/p/databases/d/documents/users/alice",
		Fields: map[string]*firestorev1.Value{
			"name": {ValueType: &firestorev1.Value_StringValue{StringValue: "Alice"}},
			"age":  {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 30}},
		},
		CreateTime: timestamppb.New(now),
		UpdateTime: timestamppb.New(now),
	}

	sd, err := codec.ProtoToStore(proto)
	require.NoError(t, err)
	assert.Equal(t, proto.Name, sd.Path)
	assert.NotEmpty(t, sd.Data)
	assert.Equal(t, int64(1), sd.Version)
	assert.Equal(t, now.Unix(), sd.CreatedAt.Unix())

	back, err := codec.StoreToProto(sd)
	require.NoError(t, err)
	assert.Equal(t, proto.Name, back.Name)
	require.Contains(t, back.Fields, "name")
	assert.Equal(t, "Alice", back.Fields["name"].GetStringValue())
	require.Contains(t, back.Fields, "age")
	assert.Equal(t, int64(30), back.Fields["age"].GetIntegerValue())
}

func TestStoreToProto_EmptyData(t *testing.T) {
	sd := &store.Document{
		Path:      "projects/p/databases/d/documents/col/doc",
		Data:      "{}",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
		Version:   1,
	}
	doc, err := codec.StoreToProto(sd)
	require.NoError(t, err)
	assert.Equal(t, sd.Path, doc.Name)
	assert.Empty(t, doc.Fields)
}

func TestProtoToStore_ZeroCreateTime(t *testing.T) {
	doc := &firestorev1.Document{
		Name:       "projects/p/databases/d/documents/col/doc",
		CreateTime: &timestamppb.Timestamp{}, // zero value: seconds=0, nanos=0
	}
	sd, err := codec.ProtoToStore(doc)
	require.NoError(t, err)
	assert.True(t, sd.CreatedAt.Year() > 1, "zero CreateTime must not propagate; got %v", sd.CreatedAt)
}

func TestStoreToProto_EmptyStringData(t *testing.T) {
	sd := &store.Document{
		Path:      "projects/p/databases/d/documents/col/doc",
		Data:      "",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
		Version:   1,
	}
	doc, err := codec.StoreToProto(sd)
	require.NoError(t, err)
	assert.Equal(t, sd.Path, doc.Name)
	assert.Empty(t, doc.Fields)
}
