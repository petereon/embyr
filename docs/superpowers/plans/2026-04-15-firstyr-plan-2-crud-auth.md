# firstyr — Plan 2: Document CRUD + Auth Middleware

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the five core Firestore document RPCs (CreateDocument, GetDocument, UpdateDocument, DeleteDocument, ListDocuments) on both SQLite and PostgreSQL adapters, and wire all four auth modes (none, key, google, mtls) as gRPC interceptors.

**Architecture:** A neutral `store.Document` type decouples the adapters from proto types; a `codec` package converts between proto and the store representation. Auth is a gRPC interceptor chain built at server construction time — the server handler layer (`handlers.go`) delegates directly to the adapter, which returns gRPC status errors so no translation layer is needed.

**Tech Stack:** Go 1.25, `google.golang.org/protobuf/encoding/protojson` (field serialisation), `modernc.org/sqlite` (SQLite CRUD + `RETURNING`), `pgx/v5/pgconn` (PostgreSQL error codes), `google.golang.org/grpc/metadata` + `peer` + `credentials` (auth), `net/http` (Google token introspection)

> **Module path:** `github.com/petereon/firstyr`
> **Working branch:** `plan-1-foundation` (continue on this branch)
> **This is Plan 2 of 5.** Builds directly on Plan 1's foundation.

---

## File Map

```
internal/
├── store/
│   ├── doc.go            NEW  — Document, ListPage, WriteMode types
│   ├── page.go           NEW  — page token encode/decode helpers (shared by both adapters)
│   ├── adapter.go        MOD  — add CRUD methods to StorageAdapter interface
│   ├── sqlite/
│   │   ├── sqlite.go     MOD  — implement CreateDocument, GetDocument, UpdateDocument,
│   │   │                        DeleteDocument, ListDocuments
│   │   └── sqlite_test.go  MOD  — add CRUD tests
│   └── postgres/
│       ├── postgres.go   MOD  — same five methods for PostgreSQL
│       └── postgres_test.go  MOD  — add CRUD tests (gate-skipped if no DSN)
├── codec/
│   ├── doc.go            NEW  — ProtoToStore, StoreToProto, ParsePath, BuildPath, NewDocumentID
│   └── doc_test.go       NEW  — codec round-trip and path parsing tests
├── auth/
│   ├── interceptor.go    NEW  — auth.Config struct + New(), 4 interceptor pairs
│   └── interceptor_test.go  NEW  — interceptor tests for none/key/google/mtls
└── server/
    ├── server.go         MOD  — wire auth.New(), ChainUnaryInterceptor, remove firestoreServer def
    └── handlers.go       NEW  — firestoreServer struct + CRUD RPC implementations
```

---

## Task 1: Internal document type, WriteMode, and StorageAdapter extension

**Files:**
- Create: `internal/store/doc.go`
- Create: `internal/store/page.go`
- Modify: `internal/store/adapter.go`

- [ ] **Step 1: Write failing compile test**

```go
// internal/store/doc_compile_test.go  (delete after Step 4)
package store_test

import (
    "testing"
    "github.com/petereon/firstyr/internal/store"
)

func TestDocumentTypeExists(t *testing.T) {
    _ = &store.Document{Path: "p", Data: "{}", Version: 1}
    _ = store.WriteModeUpsert
    _ = store.WriteModeUpdate
    _ = store.WriteModeInsertOnly
    _ = &store.ListPage{}
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd /Users/peter.vyboch/utilities/firstyr
go test ./internal/store/... 2>&1
```

Expected: compilation error — `store.Document` undefined.

- [ ] **Step 3: Create `internal/store/doc.go`**

```go
package store

import "time"

// WriteMode controls the existence precondition for UpdateDocument.
type WriteMode int8

const (
    // WriteModeUpsert creates the document if absent, updates if present.
    WriteModeUpsert WriteMode = iota
    // WriteModeUpdate requires the document to already exist.
    WriteModeUpdate
    // WriteModeInsertOnly requires the document to be absent (pure create).
    WriteModeInsertOnly
)

// Document is the backend-neutral representation of a Firestore document.
// Data is a protojson-encoded blob: {"fields":{"k":{"stringValue":"v"}}}.
// This format round-trips all Firestore Value types losslessly.
type Document struct {
    Path      string
    Data      string    // protojson {"fields":{...}}, or "{}" for empty docs
    CreatedAt time.Time
    UpdatedAt time.Time
    Version   int64
}

// ListPage is the result of a ListDocuments call.
type ListPage struct {
    Documents     []*Document
    NextPageToken string // empty string = last page
}
```

- [ ] **Step 4: Create `internal/store/page.go`**

```go
package store

import (
    "encoding/base64"
    "strconv"
)

// EncodePageToken encodes an integer offset as an opaque page token.
func EncodePageToken(offset int) string {
    return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// DecodePageToken decodes a page token back to an offset.
// Returns 0 for an empty or invalid token.
func DecodePageToken(token string) int {
    if token == "" {
        return 0
    }
    b, err := base64.StdEncoding.DecodeString(token)
    if err != nil {
        return 0
    }
    n, err := strconv.Atoi(string(b))
    if err != nil || n < 0 {
        return 0
    }
    return n
}
```

- [ ] **Step 5: Extend `internal/store/adapter.go`**

Replace the entire file:

```go
package store

import "context"

// StorageAdapter is the single interface both the SQLite and PostgreSQL backends implement.
// Methods are added incrementally across plans:
//   - Plan 1: Ping, Close, Migrate
//   - Plan 2: document CRUD (CreateDocument, GetDocument, UpdateDocument,
//             DeleteDocument, ListDocuments)
//   - Plan 3: RunQuery, BeginTransaction, CommitTransaction, RollbackTransaction, BatchWrite
//   - Plan 4: WatchCollection, UnwatchCollection
type StorageAdapter interface {
    // Ping checks that the database is reachable. Used by /readyz.
    Ping(ctx context.Context) error

    // Migrate runs all pending schema migrations.
    Migrate(ctx context.Context) error

    // Close releases all database connections.
    Close() error

    // CreateDocument inserts doc. Returns codes.AlreadyExists if doc.Path is taken.
    CreateDocument(ctx context.Context, doc *Document) (*Document, error)

    // GetDocument fetches the document at path.
    // Returns a gRPC codes.NotFound status error if the document is absent.
    GetDocument(ctx context.Context, path string) (*Document, error)

    // UpdateDocument writes doc according to mode:
    //   WriteModeUpsert     — create or update (no existence requirement)
    //   WriteModeUpdate     — must exist; returns codes.NotFound if absent
    //   WriteModeInsertOnly — must not exist; returns codes.AlreadyExists if present
    // The doc's Data field must contain the complete final state after any
    // field-mask merging has been applied by the caller.
    UpdateDocument(ctx context.Context, doc *Document, mode WriteMode) (*Document, error)

    // DeleteDocument removes the document at path.
    // If mustExist is true and the document is absent, returns codes.NotFound.
    // If mustExist is false, deletion is idempotent (no error if absent).
    DeleteDocument(ctx context.Context, path string, mustExist bool) error

    // ListDocuments returns documents directly under parent whose collection
    // matches collectionID. If collectionID is empty, all collections under
    // parent are returned.
    // pageSize=0 uses a server default of 100.
    // pageToken="" starts from the first page.
    ListDocuments(ctx context.Context, parent, collectionID string, pageSize int32, pageToken string) (*ListPage, error)
}
```

- [ ] **Step 6: Run tests to confirm compile errors are now only in the adapter implementations**

```bash
go build ./... 2>&1
```

Expected: errors in `sqlite.go` and `postgres.go` (missing methods on Adapter), and compile errors wherever `StorageAdapter` is used without the new methods. The `internal/store` package itself should compile cleanly.

- [ ] **Step 7: Delete the temporary compile test**

```bash
rm internal/store/doc_compile_test.go
```

- [ ] **Step 8: Commit**

```bash
git add internal/store/doc.go internal/store/page.go internal/store/adapter.go
git commit -m "feat: add Document type, WriteMode, and CRUD methods to StorageAdapter interface"
```

---

## Task 2: Document codec (proto ↔ store.Document)

**Files:**
- Create: `internal/codec/doc.go`
- Create: `internal/codec/doc_test.go`

- [ ] **Step 1: Write failing tests**

```go
// internal/codec/doc_test.go
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
        path     string
        wantColl string
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
```

- [ ] **Step 2: Run to verify tests fail**

```bash
go test ./internal/codec/... 2>&1
```

Expected: compilation error — package `codec` not found.

- [ ] **Step 3: Create `internal/codec/doc.go`**

```go
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
//   ParsePath("projects/p/databases/d/documents/users/alice")
//   → collection="users", parent="projects/p/databases/d/documents"
//
//   ParsePath("projects/p/databases/d/documents/users/alice/orders/123")
//   → collection="orders", parent="projects/p/databases/d/documents/users/alice"
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./internal/codec/... -v 2>&1
```

Expected: all 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/codec/doc.go internal/codec/doc_test.go
git commit -m "feat: add document codec — ProtoToStore, StoreToProto, ParsePath, BuildPath"
```

---

## Task 3: SQLite CRUD implementation

**Files:**
- Modify: `internal/store/sqlite/sqlite.go`
- Modify: `internal/store/sqlite/sqlite_test.go`

- [ ] **Step 1: Write failing tests**

Add these tests to `internal/store/sqlite/sqlite_test.go`. The file currently has `TestSQLiteAdapter_PingAndMigrate` and `TestSQLiteAdapter_MigrateIsIdempotent`. Append the new tests:

```go
// Add to the import block:
//   "github.com/petereon/firstyr/internal/store"
//   "github.com/stretchr/testify/assert"
//   "google.golang.org/grpc/codes"
//   "google.golang.org/grpc/status"

func TestSQLiteAdapter_CreateAndGetDocument(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    doc := &store.Document{
        Path:      "projects/p/databases/d/documents/users/alice",
        Data:      `{"fields":{"name":{"stringValue":"Alice"}}}`,
        CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
        UpdatedAt: time.Now().UTC().Truncate(time.Millisecond),
        Version:   1,
    }

    created, err := a.CreateDocument(ctx, doc)
    require.NoError(t, err)
    assert.Equal(t, doc.Path, created.Path)
    assert.Equal(t, int64(1), created.Version)
    assert.NotZero(t, created.CreatedAt)

    fetched, err := a.GetDocument(ctx, doc.Path)
    require.NoError(t, err)
    assert.Equal(t, doc.Path, fetched.Path)
    assert.Equal(t, doc.Data, fetched.Data)
    assert.Equal(t, int64(1), fetched.Version)
}

func TestSQLiteAdapter_CreateDocument_AlreadyExists(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    doc := &store.Document{
        Path:      "projects/p/databases/d/documents/col/dup",
        Data:      "{}",
        CreatedAt: time.Now().UTC(),
        UpdatedAt: time.Now().UTC(),
        Version:   1,
    }
    _, err := a.CreateDocument(ctx, doc)
    require.NoError(t, err)

    _, err = a.CreateDocument(ctx, doc)
    require.Error(t, err)
    assert.Equal(t, codes.AlreadyExists, status.Code(err))
}

func TestSQLiteAdapter_GetDocument_NotFound(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    _, err := a.GetDocument(ctx, "projects/p/databases/d/documents/col/missing")
    require.Error(t, err)
    assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestSQLiteAdapter_UpdateDocument_Upsert(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    doc := &store.Document{
        Path:      "projects/p/databases/d/documents/col/doc",
        Data:      `{"fields":{"x":{"stringValue":"v1"}}}`,
        CreatedAt: time.Now().UTC(),
        UpdatedAt: time.Now().UTC(),
        Version:   1,
    }
    // Upsert into an empty table — creates
    created, err := a.UpdateDocument(ctx, doc, store.WriteModeUpsert)
    require.NoError(t, err)
    assert.Equal(t, int64(1), created.Version)

    // Upsert again — updates and increments version
    doc.Data = `{"fields":{"x":{"stringValue":"v2"}}}`
    updated, err := a.UpdateDocument(ctx, doc, store.WriteModeUpsert)
    require.NoError(t, err)
    assert.Equal(t, int64(2), updated.Version)
    assert.Equal(t, doc.Data, updated.Data)
    // created_at must be preserved
    assert.Equal(t, created.CreatedAt.Unix(), updated.CreatedAt.Unix())
}

func TestSQLiteAdapter_UpdateDocument_MustExist_NotFound(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    doc := &store.Document{
        Path:      "projects/p/databases/d/documents/col/missing",
        Data:      "{}",
        CreatedAt: time.Now().UTC(),
        UpdatedAt: time.Now().UTC(),
        Version:   1,
    }
    _, err := a.UpdateDocument(ctx, doc, store.WriteModeUpdate)
    require.Error(t, err)
    assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestSQLiteAdapter_UpdateDocument_InsertOnly_AlreadyExists(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    doc := &store.Document{
        Path:      "projects/p/databases/d/documents/col/doc",
        Data:      "{}",
        CreatedAt: time.Now().UTC(),
        UpdatedAt: time.Now().UTC(),
        Version:   1,
    }
    _, err := a.CreateDocument(ctx, doc)
    require.NoError(t, err)

    _, err = a.UpdateDocument(ctx, doc, store.WriteModeInsertOnly)
    require.Error(t, err)
    assert.Equal(t, codes.AlreadyExists, status.Code(err))
}

func TestSQLiteAdapter_DeleteDocument(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    doc := &store.Document{
        Path:      "projects/p/databases/d/documents/col/todelete",
        Data:      "{}",
        CreatedAt: time.Now().UTC(),
        UpdatedAt: time.Now().UTC(),
        Version:   1,
    }
    _, err := a.CreateDocument(ctx, doc)
    require.NoError(t, err)

    err = a.DeleteDocument(ctx, doc.Path, true)
    require.NoError(t, err)

    _, err = a.GetDocument(ctx, doc.Path)
    assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestSQLiteAdapter_DeleteDocument_Idempotent(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    err := a.DeleteDocument(ctx, "projects/p/databases/d/documents/col/missing", false)
    assert.NoError(t, err)
}

func TestSQLiteAdapter_DeleteDocument_MustExist_NotFound(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    err := a.DeleteDocument(ctx, "projects/p/databases/d/documents/col/missing", true)
    assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestSQLiteAdapter_ListDocuments(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    parent := "projects/p/databases/d/documents"
    for _, id := range []string{"a", "b", "c"} {
        doc := &store.Document{
            Path:      parent + "/users/" + id,
            Data:      "{}",
            CreatedAt: time.Now().UTC(),
            UpdatedAt: time.Now().UTC(),
            Version:   1,
        }
        _, err := a.CreateDocument(ctx, doc)
        require.NoError(t, err)
    }

    page, err := a.ListDocuments(ctx, parent, "users", 10, "")
    require.NoError(t, err)
    assert.Len(t, page.Documents, 3)
    assert.Empty(t, page.NextPageToken)
}

func TestSQLiteAdapter_ListDocuments_Pagination(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    parent := "projects/p/databases/d/documents"
    for i := 0; i < 5; i++ {
        doc := &store.Document{
            Path:      fmt.Sprintf("%s/items/%d", parent, i),
            Data:      "{}",
            CreatedAt: time.Now().UTC(),
            UpdatedAt: time.Now().UTC(),
            Version:   1,
        }
        _, err := a.CreateDocument(ctx, doc)
        require.NoError(t, err)
    }

    page1, err := a.ListDocuments(ctx, parent, "items", 3, "")
    require.NoError(t, err)
    assert.Len(t, page1.Documents, 3)
    assert.NotEmpty(t, page1.NextPageToken)

    page2, err := a.ListDocuments(ctx, parent, "items", 3, page1.NextPageToken)
    require.NoError(t, err)
    assert.Len(t, page2.Documents, 2)
    assert.Empty(t, page2.NextPageToken)
}

// newTestAdapter creates a migrated SQLite adapter in a temp directory.
func newTestAdapter(t *testing.T) *Adapter {
    t.Helper()
    dir := t.TempDir()
    a, err := New(dir+"/test.db", "../../../migrations/sqlite")
    require.NoError(t, err)
    require.NoError(t, a.Migrate(context.Background()))
    t.Cleanup(func() { a.Close() })
    return a
}
```

Also update the existing test imports to use `newTestAdapter`. Replace the existing two tests with these equivalents:

```go
func TestSQLiteAdapter_PingAndMigrate(t *testing.T) {
    a := newTestAdapter(t)
    err := a.Ping(context.Background())
    require.NoError(t, err, "ping should succeed after migration")
}

func TestSQLiteAdapter_MigrateIsIdempotent(t *testing.T) {
    a := newTestAdapter(t)
    err := a.Migrate(context.Background())
    assert.NoError(t, err, "running migrations twice should not error")
}
```

The full replacement for `internal/store/sqlite/sqlite_test.go`:

```go
package sqlite

import (
    "context"
    "fmt"
    "testing"
    "time"

    "github.com/petereon/firstyr/internal/store"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

func newTestAdapter(t *testing.T) *Adapter {
    t.Helper()
    dir := t.TempDir()
    a, err := New(dir+"/test.db", "../../../migrations/sqlite")
    require.NoError(t, err)
    require.NoError(t, a.Migrate(context.Background()))
    t.Cleanup(func() { a.Close() })
    return a
}

func TestSQLiteAdapter_PingAndMigrate(t *testing.T) {
    a := newTestAdapter(t)
    err := a.Ping(context.Background())
    require.NoError(t, err, "ping should succeed after migration")
}

func TestSQLiteAdapter_MigrateIsIdempotent(t *testing.T) {
    a := newTestAdapter(t)
    err := a.Migrate(context.Background())
    assert.NoError(t, err, "running migrations twice should not error")
}

func TestSQLiteAdapter_CreateAndGetDocument(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    doc := &store.Document{
        Path:      "projects/p/databases/d/documents/users/alice",
        Data:      `{"fields":{"name":{"stringValue":"Alice"}}}`,
        CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
        UpdatedAt: time.Now().UTC().Truncate(time.Millisecond),
        Version:   1,
    }

    created, err := a.CreateDocument(ctx, doc)
    require.NoError(t, err)
    assert.Equal(t, doc.Path, created.Path)
    assert.Equal(t, int64(1), created.Version)
    assert.NotZero(t, created.CreatedAt)

    fetched, err := a.GetDocument(ctx, doc.Path)
    require.NoError(t, err)
    assert.Equal(t, doc.Path, fetched.Path)
    assert.Equal(t, doc.Data, fetched.Data)
    assert.Equal(t, int64(1), fetched.Version)
}

func TestSQLiteAdapter_CreateDocument_AlreadyExists(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    doc := &store.Document{
        Path:      "projects/p/databases/d/documents/col/dup",
        Data:      "{}",
        CreatedAt: time.Now().UTC(),
        UpdatedAt: time.Now().UTC(),
        Version:   1,
    }
    _, err := a.CreateDocument(ctx, doc)
    require.NoError(t, err)

    _, err = a.CreateDocument(ctx, doc)
    require.Error(t, err)
    assert.Equal(t, codes.AlreadyExists, status.Code(err))
}

func TestSQLiteAdapter_GetDocument_NotFound(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    _, err := a.GetDocument(ctx, "projects/p/databases/d/documents/col/missing")
    require.Error(t, err)
    assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestSQLiteAdapter_UpdateDocument_Upsert(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    doc := &store.Document{
        Path:      "projects/p/databases/d/documents/col/doc",
        Data:      `{"fields":{"x":{"stringValue":"v1"}}}`,
        CreatedAt: time.Now().UTC(),
        UpdatedAt: time.Now().UTC(),
        Version:   1,
    }
    created, err := a.UpdateDocument(ctx, doc, store.WriteModeUpsert)
    require.NoError(t, err)
    assert.Equal(t, int64(1), created.Version)

    doc.Data = `{"fields":{"x":{"stringValue":"v2"}}}`
    updated, err := a.UpdateDocument(ctx, doc, store.WriteModeUpsert)
    require.NoError(t, err)
    assert.Equal(t, int64(2), updated.Version)
    assert.Equal(t, doc.Data, updated.Data)
    assert.Equal(t, created.CreatedAt.Unix(), updated.CreatedAt.Unix())
}

func TestSQLiteAdapter_UpdateDocument_MustExist_NotFound(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    doc := &store.Document{
        Path:      "projects/p/databases/d/documents/col/missing",
        Data:      "{}",
        CreatedAt: time.Now().UTC(),
        UpdatedAt: time.Now().UTC(),
        Version:   1,
    }
    _, err := a.UpdateDocument(ctx, doc, store.WriteModeUpdate)
    require.Error(t, err)
    assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestSQLiteAdapter_UpdateDocument_InsertOnly_AlreadyExists(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    doc := &store.Document{
        Path:      "projects/p/databases/d/documents/col/doc",
        Data:      "{}",
        CreatedAt: time.Now().UTC(),
        UpdatedAt: time.Now().UTC(),
        Version:   1,
    }
    _, err := a.CreateDocument(ctx, doc)
    require.NoError(t, err)

    _, err = a.UpdateDocument(ctx, doc, store.WriteModeInsertOnly)
    require.Error(t, err)
    assert.Equal(t, codes.AlreadyExists, status.Code(err))
}

func TestSQLiteAdapter_DeleteDocument(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    doc := &store.Document{
        Path:      "projects/p/databases/d/documents/col/todelete",
        Data:      "{}",
        CreatedAt: time.Now().UTC(),
        UpdatedAt: time.Now().UTC(),
        Version:   1,
    }
    _, err := a.CreateDocument(ctx, doc)
    require.NoError(t, err)

    err = a.DeleteDocument(ctx, doc.Path, true)
    require.NoError(t, err)

    _, err = a.GetDocument(ctx, doc.Path)
    assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestSQLiteAdapter_DeleteDocument_Idempotent(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    err := a.DeleteDocument(ctx, "projects/p/databases/d/documents/col/missing", false)
    assert.NoError(t, err)
}

func TestSQLiteAdapter_DeleteDocument_MustExist_NotFound(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    err := a.DeleteDocument(ctx, "projects/p/databases/d/documents/col/missing", true)
    assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestSQLiteAdapter_ListDocuments(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    parent := "projects/p/databases/d/documents"
    for _, id := range []string{"a", "b", "c"} {
        doc := &store.Document{
            Path:      parent + "/users/" + id,
            Data:      "{}",
            CreatedAt: time.Now().UTC(),
            UpdatedAt: time.Now().UTC(),
            Version:   1,
        }
        _, err := a.CreateDocument(ctx, doc)
        require.NoError(t, err)
    }

    page, err := a.ListDocuments(ctx, parent, "users", 10, "")
    require.NoError(t, err)
    assert.Len(t, page.Documents, 3)
    assert.Empty(t, page.NextPageToken)
}

func TestSQLiteAdapter_ListDocuments_Pagination(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    parent := "projects/p/databases/d/documents"
    for i := 0; i < 5; i++ {
        doc := &store.Document{
            Path:      fmt.Sprintf("%s/items/%d", parent, i),
            Data:      "{}",
            CreatedAt: time.Now().UTC(),
            UpdatedAt: time.Now().UTC(),
            Version:   1,
        }
        _, err := a.CreateDocument(ctx, doc)
        require.NoError(t, err)
    }

    page1, err := a.ListDocuments(ctx, parent, "items", 3, "")
    require.NoError(t, err)
    assert.Len(t, page1.Documents, 3)
    assert.NotEmpty(t, page1.NextPageToken)

    page2, err := a.ListDocuments(ctx, parent, "items", 3, page1.NextPageToken)
    require.NoError(t, err)
    assert.Len(t, page2.Documents, 2)
    assert.Empty(t, page2.NextPageToken)
}
```

- [ ] **Step 2: Run to verify tests fail**

```bash
go test ./internal/store/sqlite/... 2>&1
```

Expected: compile error — `Adapter` does not implement `store.StorageAdapter`.

- [ ] **Step 3: Implement the five CRUD methods in `internal/store/sqlite/sqlite.go`**

Append these methods to the existing file (after the `Close` method):

```go
// ─── Document CRUD ────────────────────────────────────────────────────────────

const (
    sqlInsertDoc = `
        INSERT INTO documents (path, collection, parent, data, created_at, updated_at, version)
        VALUES (?, ?, ?, ?, ?, ?, 1)
        RETURNING path, data, created_at, updated_at, version`

    sqlGetDoc = `
        SELECT path, data, created_at, updated_at, version
        FROM documents WHERE path = ?`

    sqlUpsertDoc = `
        INSERT INTO documents (path, collection, parent, data, created_at, updated_at, version)
        VALUES (?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(path) DO UPDATE SET
            data       = excluded.data,
            updated_at = excluded.updated_at,
            version    = documents.version + 1
        RETURNING path, data, created_at, updated_at, version`

    sqlUpdateDoc = `
        UPDATE documents SET data=?, updated_at=?, version=version+1
        WHERE path=?
        RETURNING path, data, created_at, updated_at, version`

    sqlDeleteDoc = `DELETE FROM documents WHERE path = ?`
)

// sqliteParseCollection derives the collection name and parent path from a
// Firestore document path. This duplicates codec.ParsePath to avoid an import
// cycle (codec imports store; sqlite is a sub-package of store).
func sqliteParseCollection(path string) (collection, parent string) {
    parts := strings.Split(path, "/")
    docsIdx := -1
    for i, p := range parts {
        if p == "documents" {
            docsIdx = i
            break
        }
    }
    if docsIdx < 0 || docsIdx+2 >= len(parts) {
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

func scanDoc(row interface {
    Scan(dest ...any) error
}) (*store.Document, error) {
    var d store.Document
    var createdStr, updatedStr string
    if err := row.Scan(&d.Path, &d.Data, &createdStr, &updatedStr, &d.Version); err != nil {
        return nil, err
    }
    var err error
    if d.CreatedAt, err = time.Parse(time.RFC3339Nano, createdStr); err != nil {
        if d.CreatedAt, err = time.Parse("2006-01-02T15:04:05.999999999-07:00", createdStr); err != nil {
            d.CreatedAt = time.Now().UTC()
        }
    }
    if d.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedStr); err != nil {
        if d.UpdatedAt, err = time.Parse("2006-01-02T15:04:05.999999999-07:00", updatedStr); err != nil {
            d.UpdatedAt = time.Now().UTC()
        }
    }
    return &d, nil
}

// CreateDocument inserts a new document. Returns codes.AlreadyExists if path taken.
func (a *Adapter) CreateDocument(ctx context.Context, doc *store.Document) (*store.Document, error) {
    collection, parent := sqliteParseCollection(doc.Path)
    now := doc.UpdatedAt.UTC().Format(time.RFC3339Nano)
    createdAt := doc.CreatedAt.UTC().Format(time.RFC3339Nano)

    row := a.db.QueryRowContext(ctx, sqlInsertDoc,
        doc.Path, collection, parent, doc.Data, createdAt, now)
    d, err := scanDoc(row)
    if err != nil {
        if strings.Contains(err.Error(), "UNIQUE constraint failed") {
            return nil, status.Errorf(codes.AlreadyExists, "sqlite: document already exists: %s", doc.Path)
        }
        return nil, fmt.Errorf("sqlite: create document: %w", err)
    }
    return d, nil
}

// GetDocument fetches a document by path. Returns codes.NotFound if absent.
func (a *Adapter) GetDocument(ctx context.Context, path string) (*store.Document, error) {
    row := a.db.QueryRowContext(ctx, sqlGetDoc, path)
    d, err := scanDoc(row)
    if err != nil {
        if errors.Is(err, sql.ErrNoRows) {
            return nil, status.Errorf(codes.NotFound, "sqlite: document not found: %s", path)
        }
        return nil, fmt.Errorf("sqlite: get document: %w", err)
    }
    return d, nil
}

// UpdateDocument writes doc according to mode.
func (a *Adapter) UpdateDocument(ctx context.Context, doc *store.Document, mode store.WriteMode) (*store.Document, error) {
    now := doc.UpdatedAt.UTC().Format(time.RFC3339Nano)

    switch mode {
    case store.WriteModeUpsert:
        collection, parent := sqliteParseCollection(doc.Path)
        createdAt := doc.CreatedAt.UTC().Format(time.RFC3339Nano)
        row := a.db.QueryRowContext(ctx, sqlUpsertDoc,
            doc.Path, collection, parent, doc.Data, createdAt, now)
        d, err := scanDoc(row)
        if err != nil {
            return nil, fmt.Errorf("sqlite: upsert document: %w", err)
        }
        return d, nil

    case store.WriteModeUpdate:
        row := a.db.QueryRowContext(ctx, sqlUpdateDoc, doc.Data, now, doc.Path)
        d, err := scanDoc(row)
        if err != nil {
            if errors.Is(err, sql.ErrNoRows) {
                return nil, status.Errorf(codes.NotFound, "sqlite: document not found: %s", doc.Path)
            }
            return nil, fmt.Errorf("sqlite: update document: %w", err)
        }
        return d, nil

    case store.WriteModeInsertOnly:
        collection, parent := sqliteParseCollection(doc.Path)
        createdAt := doc.CreatedAt.UTC().Format(time.RFC3339Nano)
        row := a.db.QueryRowContext(ctx, sqlInsertDoc,
            doc.Path, collection, parent, doc.Data, createdAt, now)
        d, err := scanDoc(row)
        if err != nil {
            if strings.Contains(err.Error(), "UNIQUE constraint failed") {
                return nil, status.Errorf(codes.AlreadyExists, "sqlite: document already exists: %s", doc.Path)
            }
            return nil, fmt.Errorf("sqlite: insert-only document: %w", err)
        }
        return d, nil

    default:
        return nil, fmt.Errorf("sqlite: unknown write mode %d", mode)
    }
}

// DeleteDocument removes a document. Returns codes.NotFound if mustExist and absent.
func (a *Adapter) DeleteDocument(ctx context.Context, path string, mustExist bool) error {
    result, err := a.db.ExecContext(ctx, sqlDeleteDoc, path)
    if err != nil {
        return fmt.Errorf("sqlite: delete document: %w", err)
    }
    n, err := result.RowsAffected()
    if err != nil {
        return fmt.Errorf("sqlite: delete rows affected: %w", err)
    }
    if mustExist && n == 0 {
        return status.Errorf(codes.NotFound, "sqlite: document not found: %s", path)
    }
    return nil
}

// ListDocuments returns documents in a collection, paginated by offset.
func (a *Adapter) ListDocuments(ctx context.Context, parent, collectionID string, pageSize int32, pageToken string) (*store.ListPage, error) {
    if pageSize <= 0 {
        pageSize = 100
    }
    offset := store.DecodePageToken(pageToken)

    var (
        rows *sql.Rows
        err  error
    )
    if collectionID == "" {
        rows, err = a.db.QueryContext(ctx,
            `SELECT path, data, created_at, updated_at, version
             FROM documents WHERE parent = ?
             ORDER BY path ASC LIMIT ? OFFSET ?`,
            parent, pageSize, offset)
    } else {
        rows, err = a.db.QueryContext(ctx,
            `SELECT path, data, created_at, updated_at, version
             FROM documents WHERE collection = ? AND parent = ?
             ORDER BY path ASC LIMIT ? OFFSET ?`,
            collectionID, parent, pageSize, offset)
    }
    if err != nil {
        return nil, fmt.Errorf("sqlite: list documents: %w", err)
    }
    defer rows.Close()

    docs := make([]*store.Document, 0, pageSize)
    for rows.Next() {
        d, err := scanDoc(rows)
        if err != nil {
            return nil, fmt.Errorf("sqlite: scan document row: %w", err)
        }
        docs = append(docs, d)
    }
    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("sqlite: list documents rows: %w", err)
    }

    var nextToken string
    if int(pageSize) == len(docs) {
        nextToken = store.EncodePageToken(offset + int(pageSize))
    }
    return &store.ListPage{Documents: docs, NextPageToken: nextToken}, nil
}
```

Also update the import block at the top of `sqlite.go` to add:

```go
import (
    "context"
    "database/sql"
    "errors"
    "fmt"
    "strings"
    "time"

    "github.com/golang-migrate/migrate/v4"
    migratesqlite "github.com/golang-migrate/migrate/v4/database/sqlite"
    _ "github.com/golang-migrate/migrate/v4/source/file"
    _ "modernc.org/sqlite"

    "github.com/petereon/firstyr/internal/store"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./internal/store/sqlite/... -v 2>&1
```

Expected: all tests pass, including the new CRUD tests.

- [ ] **Step 5: Commit**

```bash
git add internal/store/sqlite/sqlite.go internal/store/sqlite/sqlite_test.go
git commit -m "feat: implement document CRUD on SQLite adapter"
```

---

## Task 4: PostgreSQL CRUD implementation

**Files:**
- Modify: `internal/store/postgres/postgres.go`
- Modify: `internal/store/postgres/postgres_test.go`

The PostgreSQL adapter mirrors the SQLite adapter but uses `TIMESTAMPTZ`, `JSONB`, and pgx error codes.

- [ ] **Step 1: Write failing tests**

Replace `internal/store/postgres/postgres_test.go` with:

```go
package postgres_test

import (
    "context"
    "fmt"
    "os"
    "testing"
    "time"

    "github.com/petereon/firstyr/internal/store"
    "github.com/petereon/firstyr/internal/store/postgres"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

func dsn(t *testing.T) string {
    t.Helper()
    v := os.Getenv("TEST_POSTGRES_DSN")
    if v == "" {
        t.Skip("TEST_POSTGRES_DSN not set; skipping PostgreSQL tests")
    }
    return v
}

func newTestAdapter(t *testing.T) *postgres.Adapter {
    t.Helper()
    a, err := postgres.New(dsn(t), "../../../migrations/postgres", 0)
    require.NoError(t, err)
    require.NoError(t, a.Migrate(context.Background()))
    t.Cleanup(func() { a.Close() })
    return a
}

func TestPostgresAdapter_PingAndMigrate(t *testing.T) {
    a := newTestAdapter(t)
    err := a.Ping(context.Background())
    require.NoError(t, err, "ping should succeed after migration")
}

func TestPostgresAdapter_MigrateIsIdempotent(t *testing.T) {
    a := newTestAdapter(t)
    err := a.Migrate(context.Background())
    assert.NoError(t, err, "running migrations twice should not error")
}

func TestPostgresAdapter_CreateAndGetDocument(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    path := fmt.Sprintf("projects/p/databases/d/documents/users/%d", time.Now().UnixNano())
    doc := &store.Document{
        Path:      path,
        Data:      `{"fields":{"name":{"stringValue":"Alice"}}}`,
        CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
        UpdatedAt: time.Now().UTC().Truncate(time.Millisecond),
        Version:   1,
    }

    created, err := a.CreateDocument(ctx, doc)
    require.NoError(t, err)
    assert.Equal(t, doc.Path, created.Path)
    assert.Equal(t, int64(1), created.Version)

    fetched, err := a.GetDocument(ctx, doc.Path)
    require.NoError(t, err)
    assert.Equal(t, doc.Path, fetched.Path)
    assert.Equal(t, int64(1), fetched.Version)
}

func TestPostgresAdapter_CreateDocument_AlreadyExists(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    path := fmt.Sprintf("projects/p/databases/d/documents/col/%d", time.Now().UnixNano())
    doc := &store.Document{Path: path, Data: "{}", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(), Version: 1}
    _, err := a.CreateDocument(ctx, doc)
    require.NoError(t, err)

    _, err = a.CreateDocument(ctx, doc)
    require.Error(t, err)
    assert.Equal(t, codes.AlreadyExists, status.Code(err))
}

func TestPostgresAdapter_GetDocument_NotFound(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    _, err := a.GetDocument(ctx, "projects/p/databases/d/documents/col/missing_pg")
    require.Error(t, err)
    assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestPostgresAdapter_UpdateDocument_Upsert(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    path := fmt.Sprintf("projects/p/databases/d/documents/col/%d", time.Now().UnixNano())
    doc := &store.Document{
        Path: path, Data: `{"fields":{"x":{"stringValue":"v1"}}}`,
        CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(), Version: 1,
    }
    created, err := a.UpdateDocument(ctx, doc, store.WriteModeUpsert)
    require.NoError(t, err)
    assert.Equal(t, int64(1), created.Version)

    doc.Data = `{"fields":{"x":{"stringValue":"v2"}}}`
    updated, err := a.UpdateDocument(ctx, doc, store.WriteModeUpsert)
    require.NoError(t, err)
    assert.Equal(t, int64(2), updated.Version)
    assert.Equal(t, created.CreatedAt.Unix(), updated.CreatedAt.Unix())
}

func TestPostgresAdapter_DeleteDocument_Idempotent(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    err := a.DeleteDocument(ctx, "projects/p/databases/d/documents/col/missing_pg", false)
    assert.NoError(t, err)
}

func TestPostgresAdapter_ListDocuments(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    suffix := fmt.Sprintf("%d", time.Now().UnixNano())
    parent := "projects/p/databases/d/documents"
    for _, id := range []string{"a_" + suffix, "b_" + suffix} {
        doc := &store.Document{
            Path: parent + "/pgusers/" + id, Data: "{}",
            CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(), Version: 1,
        }
        _, err := a.CreateDocument(ctx, doc)
        require.NoError(t, err)
    }

    page, err := a.ListDocuments(ctx, parent, "pgusers", 10, "")
    require.NoError(t, err)
    assert.GreaterOrEqual(t, len(page.Documents), 2)
}
```

- [ ] **Step 2: Run to verify compile error**

```bash
go test ./internal/store/postgres/... 2>&1
```

Expected: compile error — `Adapter` does not implement `store.StorageAdapter` (missing CRUD methods).

- [ ] **Step 3: Implement CRUD in `internal/store/postgres/postgres.go`**

Update the import block and append the CRUD methods. The full updated file:

```go
package postgres

import (
    "context"
    "database/sql"
    "errors"
    "fmt"
    "strings"
    "time"

    "github.com/golang-migrate/migrate/v4"
    migratepostgres "github.com/golang-migrate/migrate/v4/database/postgres"
    _ "github.com/golang-migrate/migrate/v4/source/file"
    "github.com/jackc/pgx/v5/pgconn"
    _ "github.com/jackc/pgx/v5/stdlib"

    "github.com/petereon/firstyr/internal/store"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

// Compile-time check that Adapter implements store.StorageAdapter.
var _ store.StorageAdapter = (*Adapter)(nil)

// Adapter implements store.StorageAdapter for PostgreSQL.
type Adapter struct {
    db             *sql.DB
    migrationsPath string
}

// New opens a connection pool to the PostgreSQL database at dsn and returns a ready Adapter.
// maxConns sets the maximum number of open connections (0 = database/sql default, unlimited).
func New(dsn, migrationsPath string, maxConns int) (*Adapter, error) {
    db, err := sql.Open("pgx", dsn)
    if err != nil {
        return nil, fmt.Errorf("postgres: open: %w", err)
    }
    if maxConns > 0 {
        db.SetMaxOpenConns(maxConns)
    }
    return &Adapter{db: db, migrationsPath: migrationsPath}, nil
}

// Ping verifies the database connection is alive.
func (a *Adapter) Ping(ctx context.Context) error {
    return a.db.PingContext(ctx)
}

// Migrate applies all pending up-migrations from migrationsPath.
func (a *Adapter) Migrate(ctx context.Context) error {
    driver, err := migratepostgres.WithInstance(a.db, &migratepostgres.Config{})
    if err != nil {
        return fmt.Errorf("postgres: migrate driver: %w", err)
    }
    m, err := migrate.NewWithDatabaseInstance(
        "file://"+a.migrationsPath,
        "postgres", driver,
    )
    if err != nil {
        return fmt.Errorf("postgres: migrate init: %w", err)
    }
    if err := m.Up(); err != nil && err != migrate.ErrNoChange {
        return fmt.Errorf("postgres: migrate up: %w", err)
    }
    return nil
}

// Close releases the database connection pool.
func (a *Adapter) Close() error {
    return a.db.Close()
}

// ─── Document CRUD ────────────────────────────────────────────────────────────

// pgParseCollection is a local duplicate of codec.ParsePath to avoid an
// import cycle (codec imports store; postgres is a sub-package of store).
func pgParseCollection(path string) (collection, parent string) {
    parts := strings.Split(path, "/")
    docsIdx := -1
    for i, p := range parts {
        if p == "documents" {
            docsIdx = i
            break
        }
    }
    if docsIdx < 0 || docsIdx+2 >= len(parts) {
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

// scanPgDoc scans a row from the PostgreSQL documents table.
// The data column is JSONB and comes back as []byte.
func scanPgDoc(row interface {
    Scan(dest ...any) error
}) (*store.Document, error) {
    var d store.Document
    var dataBytes []byte
    if err := row.Scan(&d.Path, &dataBytes, &d.CreatedAt, &d.UpdatedAt, &d.Version); err != nil {
        return nil, err
    }
    d.Data = string(dataBytes)
    d.CreatedAt = d.CreatedAt.UTC()
    d.UpdatedAt = d.UpdatedAt.UTC()
    return &d, nil
}

// isPgUniqueViolation reports whether err is a PostgreSQL unique constraint violation.
func isPgUniqueViolation(err error) bool {
    var pgErr *pgconn.PgError
    return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// CreateDocument inserts a new document. Returns codes.AlreadyExists if path taken.
func (a *Adapter) CreateDocument(ctx context.Context, doc *store.Document) (*store.Document, error) {
    collection, parent := pgParseCollection(doc.Path)
    row := a.db.QueryRowContext(ctx, `
        INSERT INTO documents (path, collection, parent, data, created_at, updated_at, version)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, 1)
        RETURNING path, data, created_at, updated_at, version`,
        doc.Path, collection, parent, doc.Data, doc.CreatedAt.UTC(), doc.UpdatedAt.UTC())
    d, err := scanPgDoc(row)
    if err != nil {
        if isPgUniqueViolation(err) {
            return nil, status.Errorf(codes.AlreadyExists, "postgres: document already exists: %s", doc.Path)
        }
        return nil, fmt.Errorf("postgres: create document: %w", err)
    }
    return d, nil
}

// GetDocument fetches a document by path. Returns codes.NotFound if absent.
func (a *Adapter) GetDocument(ctx context.Context, path string) (*store.Document, error) {
    row := a.db.QueryRowContext(ctx,
        `SELECT path, data, created_at, updated_at, version FROM documents WHERE path = $1`, path)
    d, err := scanPgDoc(row)
    if err != nil {
        if errors.Is(err, sql.ErrNoRows) {
            return nil, status.Errorf(codes.NotFound, "postgres: document not found: %s", path)
        }
        return nil, fmt.Errorf("postgres: get document: %w", err)
    }
    return d, nil
}

// UpdateDocument writes doc according to mode.
func (a *Adapter) UpdateDocument(ctx context.Context, doc *store.Document, mode store.WriteMode) (*store.Document, error) {
    switch mode {
    case store.WriteModeUpsert:
        collection, parent := pgParseCollection(doc.Path)
        row := a.db.QueryRowContext(ctx, `
            INSERT INTO documents (path, collection, parent, data, created_at, updated_at, version)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, 1)
            ON CONFLICT(path) DO UPDATE SET
                data       = EXCLUDED.data,
                updated_at = EXCLUDED.updated_at,
                version    = documents.version + 1
            RETURNING path, data, created_at, updated_at, version`,
            doc.Path, collection, parent, doc.Data,
            doc.CreatedAt.UTC(), time.Now().UTC())
        d, err := scanPgDoc(row)
        if err != nil {
            return nil, fmt.Errorf("postgres: upsert document: %w", err)
        }
        return d, nil

    case store.WriteModeUpdate:
        row := a.db.QueryRowContext(ctx, `
            UPDATE documents SET data=$1::jsonb, updated_at=$2, version=version+1
            WHERE path=$3
            RETURNING path, data, created_at, updated_at, version`,
            doc.Data, time.Now().UTC(), doc.Path)
        d, err := scanPgDoc(row)
        if err != nil {
            if errors.Is(err, sql.ErrNoRows) {
                return nil, status.Errorf(codes.NotFound, "postgres: document not found: %s", doc.Path)
            }
            return nil, fmt.Errorf("postgres: update document: %w", err)
        }
        return d, nil

    case store.WriteModeInsertOnly:
        collection, parent := pgParseCollection(doc.Path)
        row := a.db.QueryRowContext(ctx, `
            INSERT INTO documents (path, collection, parent, data, created_at, updated_at, version)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, 1)
            RETURNING path, data, created_at, updated_at, version`,
            doc.Path, collection, parent, doc.Data, doc.CreatedAt.UTC(), doc.UpdatedAt.UTC())
        d, err := scanPgDoc(row)
        if err != nil {
            if isPgUniqueViolation(err) {
                return nil, status.Errorf(codes.AlreadyExists, "postgres: document already exists: %s", doc.Path)
            }
            return nil, fmt.Errorf("postgres: insert-only document: %w", err)
        }
        return d, nil

    default:
        return nil, fmt.Errorf("postgres: unknown write mode %d", mode)
    }
}

// DeleteDocument removes a document. Returns codes.NotFound if mustExist and absent.
func (a *Adapter) DeleteDocument(ctx context.Context, path string, mustExist bool) error {
    result, err := a.db.ExecContext(ctx, `DELETE FROM documents WHERE path = $1`, path)
    if err != nil {
        return fmt.Errorf("postgres: delete document: %w", err)
    }
    n, err := result.RowsAffected()
    if err != nil {
        return fmt.Errorf("postgres: delete rows affected: %w", err)
    }
    if mustExist && n == 0 {
        return status.Errorf(codes.NotFound, "postgres: document not found: %s", path)
    }
    return nil
}

// ListDocuments returns documents in a collection, paginated by offset.
func (a *Adapter) ListDocuments(ctx context.Context, parent, collectionID string, pageSize int32, pageToken string) (*store.ListPage, error) {
    if pageSize <= 0 {
        pageSize = 100
    }
    offset := store.DecodePageToken(pageToken)

    var (
        rows *sql.Rows
        err  error
    )
    if collectionID == "" {
        rows, err = a.db.QueryContext(ctx,
            `SELECT path, data, created_at, updated_at, version
             FROM documents WHERE parent = $1
             ORDER BY path ASC LIMIT $2 OFFSET $3`,
            parent, pageSize, offset)
    } else {
        rows, err = a.db.QueryContext(ctx,
            `SELECT path, data, created_at, updated_at, version
             FROM documents WHERE collection = $1 AND parent = $2
             ORDER BY path ASC LIMIT $3 OFFSET $4`,
            collectionID, parent, pageSize, offset)
    }
    if err != nil {
        return nil, fmt.Errorf("postgres: list documents: %w", err)
    }
    defer rows.Close()

    docs := make([]*store.Document, 0, pageSize)
    for rows.Next() {
        d, err := scanPgDoc(rows)
        if err != nil {
            return nil, fmt.Errorf("postgres: scan document row: %w", err)
        }
        docs = append(docs, d)
    }
    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("postgres: list documents rows: %w", err)
    }

    var nextToken string
    if int(pageSize) == len(docs) {
        nextToken = store.EncodePageToken(offset + int(pageSize))
    }
    return &store.ListPage{Documents: docs, NextPageToken: nextToken}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./internal/store/postgres/... -v 2>&1
```

Expected: skipped if `TEST_POSTGRES_DSN` is not set. If set, all tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/store/postgres/postgres.go internal/store/postgres/postgres_test.go
git commit -m "feat: implement document CRUD on PostgreSQL adapter"
```

---

## Task 5: Auth interceptors

**Files:**
- Create: `internal/auth/interceptor.go`
- Create: `internal/auth/interceptor_test.go`

- [ ] **Step 1: Write failing tests**

```go
// internal/auth/interceptor_test.go
package auth_test

import (
    "context"
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "testing"

    "github.com/petereon/firstyr/internal/auth"
    "github.com/petereon/firstyr/internal/config"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "go.uber.org/zap"
    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/metadata"
    "google.golang.org/grpc/status"
)

func nopHandler(ctx context.Context, req interface{}) (interface{}, error) { return "ok", nil }

func ctxWithBearer(token string) context.Context {
    md := metadata.Pairs("authorization", "Bearer "+token)
    return metadata.NewIncomingContext(context.Background(), md)
}

func TestNoneMode_PassesAll(t *testing.T) {
    cfg := &config.Config{}
    cfg.Auth.Mode = "none"
    a, err := auth.New(cfg, zap.NewNop())
    require.NoError(t, err)

    _, err = a.Unary(context.Background(), nil, &grpc.UnaryServerInfo{}, nopHandler)
    assert.NoError(t, err)
}

func TestKeyMode_ValidKey(t *testing.T) {
    cfg := &config.Config{}
    cfg.Auth.Mode = "key"
    cfg.Auth.Key = "secret"
    a, err := auth.New(cfg, zap.NewNop())
    require.NoError(t, err)

    ctx := ctxWithBearer("secret")
    _, err = a.Unary(ctx, nil, &grpc.UnaryServerInfo{}, nopHandler)
    assert.NoError(t, err)
}

func TestKeyMode_WrongKey(t *testing.T) {
    cfg := &config.Config{}
    cfg.Auth.Mode = "key"
    cfg.Auth.Key = "secret"
    a, err := auth.New(cfg, zap.NewNop())
    require.NoError(t, err)

    ctx := ctxWithBearer("wrong")
    _, err = a.Unary(ctx, nil, &grpc.UnaryServerInfo{}, nopHandler)
    require.Error(t, err)
    assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestKeyMode_MissingHeader(t *testing.T) {
    cfg := &config.Config{}
    cfg.Auth.Mode = "key"
    cfg.Auth.Key = "secret"
    a, err := auth.New(cfg, zap.NewNop())
    require.NoError(t, err)

    _, err = a.Unary(context.Background(), nil, &grpc.UnaryServerInfo{}, nopHandler)
    require.Error(t, err)
    assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestKeyMode_EmptyKey_ConfigError(t *testing.T) {
    cfg := &config.Config{}
    cfg.Auth.Mode = "key"
    cfg.Auth.Key = ""
    _, err := auth.New(cfg, zap.NewNop())
    require.Error(t, err)
}

func TestGoogleMode_ValidToken(t *testing.T) {
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        json.NewEncoder(w).Encode(map[string]string{
            "audience": "my-project",
        })
    }))
    defer srv.Close()

    // Override the tokeninfo URL for this test
    auth.SetTokenInfoURL(srv.URL)
    defer auth.SetTokenInfoURL("https://www.googleapis.com/oauth2/v1/tokeninfo")

    cfg := &config.Config{}
    cfg.Auth.Mode = "google"
    cfg.Auth.GoogleProjectID = "my-project"
    a, err := auth.New(cfg, zap.NewNop())
    require.NoError(t, err)

    ctx := ctxWithBearer("fake-token")
    _, err = a.Unary(ctx, nil, &grpc.UnaryServerInfo{}, nopHandler)
    assert.NoError(t, err)
}

func TestGoogleMode_WrongAudience(t *testing.T) {
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        json.NewEncoder(w).Encode(map[string]string{
            "audience": "different-project",
        })
    }))
    defer srv.Close()

    auth.SetTokenInfoURL(srv.URL)
    defer auth.SetTokenInfoURL("https://www.googleapis.com/oauth2/v1/tokeninfo")

    cfg := &config.Config{}
    cfg.Auth.Mode = "google"
    cfg.Auth.GoogleProjectID = "my-project"
    a, err := auth.New(cfg, zap.NewNop())
    require.NoError(t, err)

    ctx := ctxWithBearer("fake-token")
    _, err = a.Unary(ctx, nil, &grpc.UnaryServerInfo{}, nopHandler)
    require.Error(t, err)
    assert.Equal(t, codes.Unauthenticated, status.Code(err))
}
```

- [ ] **Step 2: Run to verify tests fail**

```bash
go test ./internal/auth/... 2>&1
```

Expected: compile error — package `auth` not found.

- [ ] **Step 3: Create `internal/auth/interceptor.go`**

```go
package auth

import (
    "context"
    "crypto/tls"
    "crypto/x509"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strings"

    "github.com/petereon/firstyr/internal/config"
    "go.uber.org/zap"
    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/credentials"
    "google.golang.org/grpc/metadata"
    "google.golang.org/grpc/peer"
    "google.golang.org/grpc/status"
)

// tokenInfoURL is the Google tokeninfo endpoint. Overridable in tests via SetTokenInfoURL.
var tokenInfoURL = "https://www.googleapis.com/oauth2/v1/tokeninfo"

// SetTokenInfoURL overrides the Google tokeninfo endpoint. For testing only.
func SetTokenInfoURL(url string) { tokenInfoURL = url }

// Config holds the auth interceptors and optional TLS credentials for the gRPC server.
type Config struct {
    // Unary is the unary server interceptor for the configured auth mode.
    Unary grpc.UnaryServerInterceptor
    // Stream is the stream server interceptor for the configured auth mode.
    Stream grpc.StreamServerInterceptor
    // Creds is the transport credentials to apply to the gRPC server.
    // Nil for all modes except "mtls".
    Creds credentials.TransportCredentials
}

// New builds auth interceptors from the given configuration.
// Returns an error if the configuration is invalid (e.g. key mode with no key set).
func New(cfg *config.Config, _ *zap.Logger) (*Config, error) {
    switch cfg.Auth.Mode {
    case "none", "":
        return &Config{Unary: noneUnary, Stream: noneStream}, nil

    case "key":
        if cfg.Auth.Key == "" {
            return nil, fmt.Errorf("auth: key mode requires auth.key to be set")
        }
        return &Config{
            Unary:  keyUnary(cfg.Auth.Key),
            Stream: keyStream(cfg.Auth.Key),
        }, nil

    case "google":
        if cfg.Auth.GoogleProjectID == "" {
            return nil, fmt.Errorf("auth: google mode requires auth.google_project_id to be set")
        }
        return &Config{
            Unary:  googleUnary(cfg.Auth.GoogleProjectID),
            Stream: googleStream(cfg.Auth.GoogleProjectID),
        }, nil

    case "mtls":
        creds, err := buildMTLSCreds(cfg)
        if err != nil {
            return nil, err
        }
        return &Config{
            Unary:  mtlsUnary,
            Stream: mtlsStream,
            Creds:  creds,
        }, nil

    default:
        return nil, fmt.Errorf("auth: unknown mode %q", cfg.Auth.Mode)
    }
}

// ─── none ────────────────────────────────────────────────────────────────────

func noneUnary(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
    return handler(ctx, req)
}

func noneStream(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
    return handler(srv, ss)
}

// ─── key ─────────────────────────────────────────────────────────────────────

func keyUnary(key string) grpc.UnaryServerInterceptor {
    return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
        if err := validateKey(ctx, key); err != nil {
            return nil, err
        }
        return handler(ctx, req)
    }
}

func keyStream(key string) grpc.StreamServerInterceptor {
    return func(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
        if err := validateKey(ss.Context(), key); err != nil {
            return err
        }
        return handler(srv, ss)
    }
}

func validateKey(ctx context.Context, expectedKey string) error {
    token, err := bearerToken(ctx)
    if err != nil {
        return err
    }
    if token != expectedKey {
        return status.Error(codes.Unauthenticated, "invalid API key")
    }
    return nil
}

// ─── google ───────────────────────────────────────────────────────────────────

func googleUnary(projectID string) grpc.UnaryServerInterceptor {
    return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
        if err := validateGoogleToken(ctx, projectID); err != nil {
            return nil, err
        }
        return handler(ctx, req)
    }
}

func googleStream(projectID string) grpc.StreamServerInterceptor {
    return func(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
        if err := validateGoogleToken(ss.Context(), projectID); err != nil {
            return err
        }
        return handler(srv, ss)
    }
}

func validateGoogleToken(ctx context.Context, projectID string) error {
    token, err := bearerToken(ctx)
    if err != nil {
        return err
    }
    resp, err := http.Get(tokenInfoURL + "?id_token=" + token)
    if err != nil {
        return status.Errorf(codes.Unauthenticated, "google token validation: %v", err)
    }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusOK {
        return status.Error(codes.Unauthenticated, "invalid google token")
    }
    body, _ := io.ReadAll(resp.Body)
    var info struct {
        Audience string `json:"audience"`
        AZP      string `json:"azp"`
    }
    if err := json.Unmarshal(body, &info); err != nil {
        return status.Errorf(codes.Internal, "google token parse: %v", err)
    }
    if info.Audience != projectID && info.AZP != projectID {
        return status.Error(codes.Unauthenticated, "token audience does not match project ID")
    }
    return nil
}

// ─── mtls ─────────────────────────────────────────────────────────────────────

func buildMTLSCreds(cfg *config.Config) (credentials.TransportCredentials, error) {
    if cfg.Auth.MTLSCACert == "" {
        return nil, fmt.Errorf("auth: mtls mode requires auth.mtls_ca to be set")
    }
    if cfg.Server.TLS.Cert == "" || cfg.Server.TLS.Key == "" {
        return nil, fmt.Errorf("auth: mtls mode requires server.tls.cert and server.tls.key to be set")
    }
    caCert, err := os.ReadFile(cfg.Auth.MTLSCACert)
    if err != nil {
        return nil, fmt.Errorf("auth: read mTLS CA cert: %w", err)
    }
    caPool := x509.NewCertPool()
    if !caPool.AppendCertsFromPEM(caCert) {
        return nil, fmt.Errorf("auth: invalid mTLS CA cert PEM")
    }
    serverCert, err := tls.LoadX509KeyPair(cfg.Server.TLS.Cert, cfg.Server.TLS.Key)
    if err != nil {
        return nil, fmt.Errorf("auth: load server cert/key: %w", err)
    }
    return credentials.NewTLS(&tls.Config{
        Certificates: []tls.Certificate{serverCert},
        ClientAuth:   tls.RequireAndVerifyClientCert,
        ClientCAs:    caPool,
    }), nil
}

func mtlsUnary(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
    if err := validateMTLS(ctx); err != nil {
        return nil, err
    }
    return handler(ctx, req)
}

func mtlsStream(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
    if err := validateMTLS(ss.Context()); err != nil {
        return err
    }
    return handler(srv, ss)
}

func validateMTLS(ctx context.Context) error {
    p, ok := peer.FromContext(ctx)
    if !ok {
        return status.Error(codes.Unauthenticated, "no peer info in context")
    }
    tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
    if !ok {
        return status.Error(codes.Unauthenticated, "connection is not using TLS")
    }
    if len(tlsInfo.State.VerifiedChains) == 0 {
        return status.Error(codes.Unauthenticated, "no verified client certificate")
    }
    return nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// bearerToken extracts the Bearer token from incoming gRPC metadata.
func bearerToken(ctx context.Context) (string, error) {
    md, ok := metadata.FromIncomingContext(ctx)
    if !ok {
        return "", status.Error(codes.Unauthenticated, "missing metadata")
    }
    vals := md.Get("authorization")
    if len(vals) == 0 {
        return "", status.Error(codes.Unauthenticated, "missing authorization header")
    }
    parts := strings.SplitN(vals[0], " ", 2)
    if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
        return "", status.Error(codes.Unauthenticated, "authorization header must be 'Bearer <token>'")
    }
    return parts[1], nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./internal/auth/... -v 2>&1
```

Expected: all 7 tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/auth/interceptor.go internal/auth/interceptor_test.go
git commit -m "feat: add auth interceptors for none, key, google, and mtls modes"
```

---

## Task 6: Server refactor — wire auth + add db/log to firestoreServer

**Files:**
- Modify: `internal/server/server.go`
- Create: `internal/server/handlers.go`

- [ ] **Step 1: Create `internal/server/handlers.go`**

This file owns the `firestoreServer` struct and the five CRUD RPC implementations.

```go
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
```

- [ ] **Step 2: Update `internal/server/server.go`**

Replace the file to remove the old `firestoreServer` definition, wire auth, and use `grpc.ChainUnaryInterceptor`:

```go
package server

import (
    "context"
    "fmt"
    "net"
    "net/http"
    "time"

    firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
    "github.com/petereon/firstyr/internal/auth"
    "github.com/petereon/firstyr/internal/config"
    "github.com/petereon/firstyr/internal/health"
    "github.com/petereon/firstyr/internal/store"
    "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
    "go.uber.org/zap"
    "golang.org/x/sync/errgroup"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/grpc/reflection"
    "google.golang.org/grpc/status"
)

// Server wraps the gRPC server and the grpc-gateway REST mux.
type Server struct {
    cfg        *config.Config
    db         store.StorageAdapter
    log        *zap.Logger
    grpcServer *grpc.Server
    restMux    *http.ServeMux
}

// New creates a Server wired to db. It does not start listening.
func New(cfg *config.Config, db store.StorageAdapter, log *zap.Logger) (*Server, error) {
    authCfg, err := auth.New(cfg, log)
    if err != nil {
        return nil, fmt.Errorf("server: auth: %w", err)
    }

    opts := []grpc.ServerOption{
        grpc.ChainUnaryInterceptor(authCfg.Unary, loggingUnaryInterceptor(log)),
        grpc.ChainStreamInterceptor(authCfg.Stream, loggingStreamInterceptor(log)),
    }
    if authCfg.Creds != nil {
        opts = append(opts, grpc.Creds(authCfg.Creds))
    }

    grpcSrv := grpc.NewServer(opts...)
    firestorev1.RegisterFirestoreServer(grpcSrv, &firestoreServer{db: db, log: log})
    reflection.Register(grpcSrv)

    gwMux := runtime.NewServeMux()
    grpcAddr := fmt.Sprintf("127.0.0.1:%d", cfg.Server.GRPCPort)
    if err := firestorev1.RegisterFirestoreHandlerFromEndpoint(
        context.Background(), gwMux, grpcAddr,
        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
    ); err != nil {
        return nil, fmt.Errorf("server: register gateway: %w", err)
    }

    h := health.New(db)
    mux := http.NewServeMux()
    mux.HandleFunc("/healthz", h.Healthz)
    mux.HandleFunc("/readyz", h.Readyz)
    mux.Handle("/", gwMux)

    return &Server{
        cfg:        cfg,
        db:         db,
        log:        log,
        grpcServer: grpcSrv,
        restMux:    mux,
    }, nil
}

// Run starts both the gRPC and REST listeners. Blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
    grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cfg.Server.GRPCPort))
    if err != nil {
        return fmt.Errorf("server: gRPC listen: %w", err)
    }
    restLis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cfg.Server.RESTPort))
    if err != nil {
        return fmt.Errorf("server: REST listen: %w", err)
    }

    restSrv := &http.Server{Handler: s.restMux}

    eg, egCtx := errgroup.WithContext(ctx)
    eg.Go(func() error { return s.grpcServer.Serve(grpcLis) })
    eg.Go(func() error {
        if err := restSrv.Serve(restLis); err != nil && err != http.ErrServerClosed {
            return err
        }
        return nil
    })
    eg.Go(func() error {
        <-egCtx.Done()
        s.grpcServer.GracefulStop()
        // 5-second deadline prevents indefinite blocking if a client holds a connection open.
        shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        return restSrv.Shutdown(shutCtx)
    })
    return eg.Wait()
}

func loggingUnaryInterceptor(log *zap.Logger) grpc.UnaryServerInterceptor {
    return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
        resp, err := handler(ctx, req)
        log.Info("rpc", zap.String("method", info.FullMethod), zap.String("code", status.Code(err).String()))
        return resp, err
    }
}

func loggingStreamInterceptor(log *zap.Logger) grpc.StreamServerInterceptor {
    return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
        err := handler(srv, ss)
        log.Info("rpc_stream", zap.String("method", info.FullMethod), zap.String("code", status.Code(err).String()))
        return err
    }
}
```

- [ ] **Step 3: Build to verify it compiles**

```bash
go build ./... 2>&1
```

Expected: clean build.

- [ ] **Step 4: Run existing server test to confirm it still passes**

```bash
go test ./internal/server/... -v -run TestServer_StartsAndServes 2>&1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/server/server.go internal/server/handlers.go
git commit -m "feat: wire auth interceptors and implement CRUD RPC handlers"
```

---

## Task 7: Integration tests for CRUD RPCs

**Files:**
- Create: `internal/server/handlers_test.go`

These tests start a real server with an in-memory SQLite database, call RPCs via the REST gateway (HTTP/JSON), and verify correct responses.

- [ ] **Step 1: Write the tests**

```go
// internal/server/handlers_test.go
package server_test

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "testing"
    "time"

    "github.com/petereon/firstyr/internal/config"
    "github.com/petereon/firstyr/internal/server"
    "github.com/petereon/firstyr/internal/store/sqlite"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "go.uber.org/zap"
)

// startTestServer starts a server with a fresh SQLite database and returns the REST base URL.
func startTestServer(t *testing.T) string {
    t.Helper()
    dir := t.TempDir()
    adapter, err := sqlite.New(dir+"/test.db", "../../migrations/sqlite")
    require.NoError(t, err)
    require.NoError(t, adapter.Migrate(context.Background()))

    grpcPort := freePort(t)
    restPort := freePort(t)

    cfg := &config.Config{}
    cfg.Server.GRPCPort = grpcPort
    cfg.Server.RESTPort = restPort
    cfg.Auth.Mode = "none"

    log, _ := zap.NewDevelopment()
    srv, err := server.New(cfg, adapter, log)
    require.NoError(t, err)

    ctx, cancel := context.WithCancel(context.Background())
    t.Cleanup(func() {
        cancel()
        adapter.Close()
    })
    go srv.Run(ctx) //nolint:errcheck

    // Wait for server to be ready.
    restBase := fmt.Sprintf("http://127.0.0.1:%d", restPort)
    require.Eventually(t, func() bool {
        resp, err := http.Get(restBase + "/healthz")
        if err != nil {
            return false
        }
        resp.Body.Close()
        return resp.StatusCode == http.StatusOK
    }, 3*time.Second, 50*time.Millisecond, "server did not become ready")

    return restBase
}

func TestRPC_CreateAndGetDocument(t *testing.T) {
    base := startTestServer(t)

    // CreateDocument via REST: POST /v1/{parent}/{collectionId}
    body := `{"fields":{"name":{"stringValue":"Alice"},"age":{"integerValue":"30"}}}`
    url := base + "/v1/projects/p/databases/d/documents/users"
    resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
    require.NoError(t, err)
    defer resp.Body.Close()
    require.Equal(t, http.StatusOK, resp.StatusCode, "CreateDocument should return 200")

    var created map[string]interface{}
    require.NoError(t, json.NewDecoder(resp.Body).Decode(&created))
    name, ok := created["name"].(string)
    require.True(t, ok, "response must have a 'name' field")
    assert.Contains(t, name, "projects/p/databases/d/documents/users/")

    // GetDocument via REST: GET /v1/{name}
    getResp, err := http.Get(base + "/v1/" + name)
    require.NoError(t, err)
    defer getResp.Body.Close()
    assert.Equal(t, http.StatusOK, getResp.StatusCode)

    var fetched map[string]interface{}
    require.NoError(t, json.NewDecoder(getResp.Body).Decode(&fetched))
    assert.Equal(t, name, fetched["name"])
}

func TestRPC_CreateDocument_AlreadyExists(t *testing.T) {
    base := startTestServer(t)

    // CreateDocument with explicit document ID
    body := `{"fields":{}}`
    url := base + "/v1/projects/p/databases/d/documents/col?documentId=fixed-id"
    resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
    require.NoError(t, err)
    resp.Body.Close()
    require.Equal(t, http.StatusOK, resp.StatusCode)

    // Second create with the same ID must fail with 409
    resp2, err := http.Post(url, "application/json", bytes.NewBufferString(body))
    require.NoError(t, err)
    defer resp2.Body.Close()
    assert.Equal(t, http.StatusConflict, resp2.StatusCode)
}

func TestRPC_GetDocument_NotFound(t *testing.T) {
    base := startTestServer(t)

    resp, err := http.Get(base + "/v1/projects/p/databases/d/documents/col/no-such-doc")
    require.NoError(t, err)
    defer resp.Body.Close()
    assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestRPC_UpdateDocument_Upsert(t *testing.T) {
    base := startTestServer(t)

    // PATCH via REST maps to UpdateDocument
    docName := "projects/p/databases/d/documents/col/mydoc"
    body := `{"name":"` + docName + `","fields":{"x":{"stringValue":"hello"}}}`

    req, err := http.NewRequest(http.MethodPatch, base+"/v1/"+docName, bytes.NewBufferString(body))
    require.NoError(t, err)
    req.Header.Set("Content-Type", "application/json")

    resp, err := http.DefaultClient.Do(req)
    require.NoError(t, err)
    defer resp.Body.Close()
    assert.Equal(t, http.StatusOK, resp.StatusCode)

    var result map[string]interface{}
    require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
    assert.Equal(t, docName, result["name"])
}

func TestRPC_DeleteDocument(t *testing.T) {
    base := startTestServer(t)

    // Create a document
    body := `{"fields":{}}`
    url := base + "/v1/projects/p/databases/d/documents/col?documentId=todelete"
    resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
    require.NoError(t, err)
    resp.Body.Close()
    require.Equal(t, http.StatusOK, resp.StatusCode)

    // Delete it
    docName := "projects/p/databases/d/documents/col/todelete"
    req, err := http.NewRequest(http.MethodDelete, base+"/v1/"+docName, nil)
    require.NoError(t, err)
    delResp, err := http.DefaultClient.Do(req)
    require.NoError(t, err)
    defer delResp.Body.Close()
    assert.Equal(t, http.StatusOK, delResp.StatusCode)

    // Confirm it's gone
    getResp, err := http.Get(base + "/v1/" + docName)
    require.NoError(t, err)
    defer getResp.Body.Close()
    assert.Equal(t, http.StatusNotFound, getResp.StatusCode)
}

func TestRPC_ListDocuments(t *testing.T) {
    base := startTestServer(t)

    // Create a few documents
    for _, id := range []string{"a", "b", "c"} {
        body := `{"fields":{}}`
        url := base + "/v1/projects/p/databases/d/documents/things?documentId=" + id
        resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
        require.NoError(t, err)
        resp.Body.Close()
        require.Equal(t, http.StatusOK, resp.StatusCode)
    }

    // List them
    listResp, err := http.Get(base + "/v1/projects/p/databases/d/documents/things")
    require.NoError(t, err)
    defer listResp.Body.Close()
    assert.Equal(t, http.StatusOK, listResp.StatusCode)

    raw, _ := io.ReadAll(listResp.Body)
    var result map[string]interface{}
    require.NoError(t, json.Unmarshal(raw, &result))
    docs, ok := result["documents"].([]interface{})
    require.True(t, ok, "response must have 'documents' array; got: %s", string(raw))
    assert.Len(t, docs, 3)
}
```

- [ ] **Step 2: Run all server tests**

```bash
go test ./internal/server/... -v 2>&1
```

Expected: `TestServer_StartsAndServes` and all 6 new handler tests pass.

- [ ] **Step 3: Run the full test suite**

```bash
go test ./... 2>&1
```

Expected: all packages pass, PostgreSQL tests skipped if no DSN.

- [ ] **Step 4: Commit**

```bash
git add internal/server/handlers_test.go
git commit -m "test: add integration tests for CRUD RPCs via REST gateway"
```

---

## Verification Checklist

Before declaring Plan 2 complete, confirm all of the following:

- [ ] `go build ./...` produces no errors
- [ ] `go test ./...` passes with zero failures (PostgreSQL tests skipped if no `TEST_POSTGRES_DSN`)
- [ ] `go vet ./...` produces no output
- [ ] SQLite adapter: CreateDocument, GetDocument, UpdateDocument (upsert + update), DeleteDocument, ListDocuments all tested
- [ ] PostgreSQL adapter: same five methods compile and pass when `TEST_POSTGRES_DSN` is set
- [ ] Auth interceptors: none, key (valid/invalid/missing), google (valid/wrong audience), mtls logic all tested
- [ ] Server: `TestServer_StartsAndServes` still passes after the refactor
- [ ] REST handler tests: create, create-duplicate, get, get-not-found, update, delete, list all exercise the real gateway
- [ ] Binary (`./firstyr`) starts, `/healthz` returns `ok`, CreateDocument round-trip works via curl

---

## Up Next: Plan 3 — Query Planner + Transactions

Plan 3 will implement:
- `RunQuery` with `StructuredQuery` (field filters, orderBy, limit, cursors)
- Query index validation (`FAILED_PRECONDITION` on missing index)
- `BeginTransaction` / `Commit` / `Rollback` with optimistic concurrency (version checking)
- `BatchWrite` with per-write `exists` preconditions
- `CollectionGroup` queries
- Expired transaction sweep goroutine
