# embyr Plan 3 — Advanced Queries, Field Transforms, Transactions, BatchWrite

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the Firestore mutation and query surface: structured queries with filters/orderBy/cursors, field transforms (serverTimestamp, increment, arrayUnion, arrayRemove), read-write transactions with optimistic concurrency, and BatchWrite.

**Architecture:** Query translation lives in new `store/query.go` (types) and `codec/query.go` (proto→store conversion); each adapter adds `QueryDocuments` with SQL builders. Field transforms are applied to the proto fields map in `handlers.go` before codec encoding — no adapter changes needed for transforms. Transactions use the existing `transactions` table; a `WithTransaction` adapter method wraps any number of existing CRUD calls in a SQL transaction for OCC-free atomic batches.

**Tech Stack:** Go, `database/sql`, SQLite `json_extract` / PostgreSQL `jsonb` operators, existing `grpc/status` error mapping.

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `internal/store/query.go` | **Create** | FilterValue, Filter, OrderBy, Cursor, Query types |
| `internal/codec/query.go` | **Create** | Convert `StructuredQuery` proto → `store.Query` |
| `internal/store/adapter.go` | **Modify** | Add `QueryDocuments`, `WithTransaction`, `BeginTransaction`, `GetDocumentForTransaction`, `CommitTransaction`, `RollbackTransaction` |
| `internal/store/doc.go` | **Modify** | Add `WriteOp`, `CommitResult`, `WriteResult` types |
| `internal/store/sqlite/sqlite.go` | **Modify** | Implement all new adapter methods + query helpers |
| `internal/store/postgres/postgres.go` | **Modify** | Implement all new adapter methods + query helpers |
| `internal/server/handlers.go` | **Modify** | Update `RunQuery` (call `QueryDocuments`), add transforms to `Commit`, add `BatchWrite` |
| `internal/server/transactions.go` | **Create** | `BeginTransaction`, `Rollback` RPC handlers |

---

## Task 1: Query and filter value types

**Files:**
- Create: `internal/store/query.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/store/query_test.go
package store_test

import (
    "testing"
    "github.com/petereon/embyr/internal/store"
    "github.com/stretchr/testify/require"
)

func TestFilterValue_ZeroValues(t *testing.T) {
    fv := store.FilterValue{Kind: store.FilterValueNull}
    require.Equal(t, store.FilterValueNull, fv.Kind)

    fv2 := store.FilterValue{Kind: store.FilterValueInt, IntVal: 42}
    require.Equal(t, int64(42), fv2.IntVal)
}
```

- [ ] **Step 2: Run test, verify it fails**

```
go test ./internal/store/... -run TestFilterValue -v
```
Expected: FAIL — `store.FilterValue` undefined.

- [ ] **Step 3: Create `internal/store/query.go`**

```go
package store

import "time"

// FilterValueKind identifies the active field in a FilterValue.
type FilterValueKind int8

const (
    FilterValueNull   FilterValueKind = iota
    FilterValueBool
    FilterValueInt
    FilterValueDouble
    FilterValueString
    FilterValueTime
    FilterValueArray  // used only in IN / array-contains-any operands
)

// FilterValue is a typed scalar or array value for query filter predicates.
// Only the field matching Kind is meaningful.
type FilterValue struct {
    Kind      FilterValueKind
    BoolVal   bool
    IntVal    int64
    DoubleVal float64
    StrVal    string
    TimeVal   time.Time
    // ArrayVals holds scalar FilterValues for IN / array-contains-any operators.
    ArrayVals []FilterValue
}

// FilterOp is a Firestore filter comparison operator.
type FilterOp string

const (
    FilterOpEqual              FilterOp = "=="
    FilterOpNotEqual           FilterOp = "!="
    FilterOpLessThan           FilterOp = "<"
    FilterOpLessThanOrEqual    FilterOp = "<="
    FilterOpGreaterThan        FilterOp = ">"
    FilterOpGreaterThanOrEqual FilterOp = ">="
    FilterOpIn                 FilterOp = "in"
    FilterOpNotIn              FilterOp = "not-in"
    FilterOpArrayContains      FilterOp = "array-contains"
    FilterOpArrayContainsAny   FilterOp = "array-contains-any"
)

// FieldFilter is a single Firestore field predicate.
type FieldFilter struct {
    Field string      // dot-separated Firestore field path, e.g. "address.city"
    Op    FilterOp
    Value FilterValue
}

// CompositeFilter holds AND-combined field filters.
type CompositeFilter struct {
    Filters []FieldFilter
}

// Direction is a sort order for OrderBy clauses.
type Direction string

const (
    DirectionAsc  Direction = "ASC"
    DirectionDesc Direction = "DESC"
)

// OrderBy specifies a sort field and direction.
type OrderBy struct {
    Field     string
    Direction Direction
}

// Cursor is a query boundary (startAt / startAfter / endAt / endBefore).
type Cursor struct {
    Values []FilterValue // one value per OrderBy field
    Before bool          // true = inclusive (at); false = exclusive (after/before)
    IsEnd  bool          // true = end boundary; false = start boundary
}

// Query is the full structured query passed to StorageAdapter.QueryDocuments.
type Query struct {
    Parent       string
    CollectionID string
    Filter       *CompositeFilter
    OrderBy      []OrderBy
    Limit        int32  // 0 = no adapter-side limit; caller sets PageSize for pagination
    StartCursor  *Cursor
    EndCursor    *Cursor
    PageToken    string
    PageSize     int32
}
```

- [ ] **Step 4: Run test, verify it passes**

```
go test ./internal/store/... -run TestFilterValue -v
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store/query.go internal/store/query_test.go
git commit -m "feat(store): add query/filter/orderby/cursor types for plan 3"
```

---

## Task 2: Codec query helpers (proto → store.Query)

**Files:**
- Create: `internal/codec/query.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/codec/query_test.go
package codec_test

import (
    "testing"

    firestorev1 "github.com/petereon/embyr/gen/go/google/firestore/v1"
    "github.com/petereon/embyr/internal/codec"
    "github.com/petereon/embyr/internal/store"
    "github.com/stretchr/testify/require"
)

func TestFilterValueFromProto_String(t *testing.T) {
    v := &firestorev1.Value{ValueType: &firestorev1.Value_StringValue{StringValue: "hello"}}
    fv := codec.FilterValueFromProto(v)
    require.Equal(t, store.FilterValueString, fv.Kind)
    require.Equal(t, "hello", fv.StrVal)
}

func TestFilterValueFromProto_Int(t *testing.T) {
    v := &firestorev1.Value{ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 42}}
    fv := codec.FilterValueFromProto(v)
    require.Equal(t, store.FilterValueInt, fv.Kind)
    require.Equal(t, int64(42), fv.IntVal)
}

func TestQueryFromStructuredQuery_Filter(t *testing.T) {
    sq := &firestorev1.StructuredQuery{
        From: []*firestorev1.StructuredQuery_CollectionSelector{
            {CollectionId: "notes"},
        },
        Where: &firestorev1.StructuredQuery_Filter{
            FilterType: &firestorev1.StructuredQuery_Filter_FieldFilter{
                FieldFilter: &firestorev1.StructuredQuery_FieldFilter{
                    Field: &firestorev1.StructuredQuery_FieldReference{FieldPath: "status"},
                    Op:    firestorev1.StructuredQuery_FieldFilter_EQUAL,
                    Value: &firestorev1.Value{ValueType: &firestorev1.Value_StringValue{StringValue: "active"}},
                },
            },
        },
    }
    q, err := codec.QueryFromStructuredQuery("projects/p/databases/d/documents", sq, 0, "")
    require.NoError(t, err)
    require.NotNil(t, q.Filter)
    require.Len(t, q.Filter.Filters, 1)
    require.Equal(t, "status", q.Filter.Filters[0].Field)
    require.Equal(t, store.FilterOpEqual, q.Filter.Filters[0].Op)
    require.Equal(t, "active", q.Filter.Filters[0].Value.StrVal)
}
```

- [ ] **Step 2: Run test, verify it fails**

```
go test ./internal/codec/... -run TestFilterValue -run TestQueryFrom -v
```
Expected: FAIL — `codec.FilterValueFromProto` undefined.

- [ ] **Step 3: Create `internal/codec/query.go`**

```go
package codec

import (
    firestorev1 "github.com/petereon/embyr/gen/go/google/firestore/v1"
    "github.com/petereon/embyr/internal/store"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

// FilterValueFromProto converts a Firestore Value proto to a store.FilterValue.
func FilterValueFromProto(v *firestorev1.Value) store.FilterValue {
    if v == nil {
        return store.FilterValue{Kind: store.FilterValueNull}
    }
    switch t := v.GetValueType().(type) {
    case *firestorev1.Value_NullValue:
        return store.FilterValue{Kind: store.FilterValueNull}
    case *firestorev1.Value_BooleanValue:
        return store.FilterValue{Kind: store.FilterValueBool, BoolVal: t.BooleanValue}
    case *firestorev1.Value_IntegerValue:
        return store.FilterValue{Kind: store.FilterValueInt, IntVal: t.IntegerValue}
    case *firestorev1.Value_DoubleValue:
        return store.FilterValue{Kind: store.FilterValueDouble, DoubleVal: t.DoubleValue}
    case *firestorev1.Value_StringValue:
        return store.FilterValue{Kind: store.FilterValueString, StrVal: t.StringValue}
    case *firestorev1.Value_TimestampValue:
        return store.FilterValue{Kind: store.FilterValueTime, TimeVal: t.TimestampValue.AsTime()}
    case *firestorev1.Value_ArrayValue:
        vals := t.ArrayValue.GetValues()
        arr := make([]store.FilterValue, len(vals))
        for i, el := range vals {
            arr[i] = FilterValueFromProto(el)
        }
        return store.FilterValue{Kind: store.FilterValueArray, ArrayVals: arr}
    default:
        // Reference, bytes, geopoint — coerce to string
        return store.FilterValue{Kind: store.FilterValueString, StrVal: v.String()}
    }
}

// QueryFromStructuredQuery builds a store.Query from a Firestore StructuredQuery proto.
// parent is the Firestore resource path of the parent document/database.
// limit and pageToken are passed through from the enclosing RunQueryRequest/ListDocumentsRequest.
func QueryFromStructuredQuery(parent string, sq *firestorev1.StructuredQuery, limit int32, pageToken string) (*store.Query, error) {
    froms := sq.GetFrom()
    if len(froms) == 0 {
        return nil, status.Error(codes.InvalidArgument, "structured_query.from is required")
    }
    if len(froms) > 1 {
        return nil, status.Error(codes.Unimplemented, "collection group queries are not yet supported")
    }
    collectionID := froms[0].GetCollectionId()

    filter, err := protoCompositeFilter(sq.GetWhere())
    if err != nil {
        return nil, err
    }

    orderBys := protoOrderBys(sq.GetOrderBy())

    q := &store.Query{
        Parent:       parent,
        CollectionID: collectionID,
        Filter:       filter,
        OrderBy:      orderBys,
        PageToken:    pageToken,
    }

    if lim := sq.GetLimit(); lim != nil && lim.GetValue() > 0 {
        q.Limit = lim.GetValue()
        q.PageSize = lim.GetValue()
    } else if limit > 0 {
        q.PageSize = limit
    } else {
        q.PageSize = 300
    }

    if sc := sq.GetStartAt(); sc != nil {
        q.StartCursor = protoCursor(sc, orderBys, false)
    }
    if sc := sq.GetEndAt(); sc != nil {
        q.EndCursor = protoCursor(sc, orderBys, true)
    }

    return q, nil
}

func protoCompositeFilter(f *firestorev1.StructuredQuery_Filter) (*store.CompositeFilter, error) {
    if f == nil {
        return nil, nil
    }
    if cf := f.GetCompositeFilter(); cf != nil {
        if cf.GetOp() != firestorev1.StructuredQuery_CompositeFilter_AND {
            return nil, status.Error(codes.Unimplemented, "only AND composite filters are supported")
        }
        filters := make([]store.FieldFilter, 0, len(cf.GetFilters()))
        for _, sub := range cf.GetFilters() {
            ff, err := protoFieldFilter(sub)
            if err != nil {
                return nil, err
            }
            filters = append(filters, ff)
        }
        return &store.CompositeFilter{Filters: filters}, nil
    }
    // Single field filter at top level
    ff, err := protoFieldFilter(f)
    if err != nil {
        return nil, err
    }
    return &store.CompositeFilter{Filters: []store.FieldFilter{ff}}, nil
}

func protoFieldFilter(f *firestorev1.StructuredQuery_Filter) (store.FieldFilter, error) {
    ff := f.GetFieldFilter()
    if ff == nil {
        return store.FieldFilter{}, status.Error(codes.InvalidArgument, "expected a field filter")
    }
    op, err := protoFilterOp(ff.GetOp())
    if err != nil {
        return store.FieldFilter{}, err
    }
    return store.FieldFilter{
        Field: ff.GetField().GetFieldPath(),
        Op:    op,
        Value: FilterValueFromProto(ff.GetValue()),
    }, nil
}

func protoFilterOp(op firestorev1.StructuredQuery_FieldFilter_Operator) (store.FilterOp, error) {
    m := map[firestorev1.StructuredQuery_FieldFilter_Operator]store.FilterOp{
        firestorev1.StructuredQuery_FieldFilter_EQUAL:                store.FilterOpEqual,
        firestorev1.StructuredQuery_FieldFilter_NOT_EQUAL:            store.FilterOpNotEqual,
        firestorev1.StructuredQuery_FieldFilter_LESS_THAN:            store.FilterOpLessThan,
        firestorev1.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL:   store.FilterOpLessThanOrEqual,
        firestorev1.StructuredQuery_FieldFilter_GREATER_THAN:         store.FilterOpGreaterThan,
        firestorev1.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL: store.FilterOpGreaterThanOrEqual,
        firestorev1.StructuredQuery_FieldFilter_IN:                   store.FilterOpIn,
        firestorev1.StructuredQuery_FieldFilter_NOT_IN:               store.FilterOpNotIn,
        firestorev1.StructuredQuery_FieldFilter_ARRAY_CONTAINS:       store.FilterOpArrayContains,
        firestorev1.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY:   store.FilterOpArrayContainsAny,
    }
    so, ok := m[op]
    if !ok {
        return "", status.Errorf(codes.Unimplemented, "filter operator %v not supported", op)
    }
    return so, nil
}

func protoOrderBys(orders []*firestorev1.StructuredQuery_Order) []store.OrderBy {
    if len(orders) == 0 {
        return nil
    }
    result := make([]store.OrderBy, len(orders))
    for i, o := range orders {
        dir := store.DirectionAsc
        if o.GetDirection() == firestorev1.StructuredQuery_DESCENDING {
            dir = store.DirectionDesc
        }
        result[i] = store.OrderBy{Field: o.GetField().GetFieldPath(), Direction: dir}
    }
    return result
}

func protoCursor(c *firestorev1.Cursor, orders []store.OrderBy, isEnd bool) *store.Cursor {
    vals := c.GetValues()
    fvs := make([]store.FilterValue, len(vals))
    for i, v := range vals {
        fvs[i] = FilterValueFromProto(v)
    }
    return &store.Cursor{
        Values: fvs,
        Before: c.GetBefore(),
        IsEnd:  isEnd,
    }
}
```

- [ ] **Step 4: Run tests, verify they pass**

```
go test ./internal/codec/... -run TestFilterValue -run TestQueryFrom -v
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/codec/query.go internal/codec/query_test.go
git commit -m "feat(codec): add proto→store query conversion helpers"
```

---

## Task 3: StorageAdapter extensions + WriteOp types

**Files:**
- Modify: `internal/store/adapter.go`
- Modify: `internal/store/doc.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/store/adapter_test.go — add this test to verify the interface compiles
package store_test

import (
    "testing"
    "github.com/petereon/embyr/internal/store"
)

// TestStorageAdapterInterface verifies the expected methods exist on the interface.
// This is a compile-time check; if it compiles, it passes.
func TestStorageAdapterInterface(t *testing.T) {
    var _ store.StorageAdapter = (store.StorageAdapter)(nil)
}
```

Run:
```
go test ./internal/store/... -run TestStorageAdapterInterface -v
```
Expected: PASS (or FAIL if new methods break existing adapters — that's expected until Task 4/5 implement them).

- [ ] **Step 2: Add `WriteOp`, `CommitResult`, `WriteResult` to `internal/store/doc.go`**

Append to the end of `internal/store/doc.go`:

```go
// WriteOpType identifies the kind of write in a WriteOp.
type WriteOpType int8

const (
    WriteOpUpdate WriteOpType = iota // create or update
    WriteOpDelete                    // delete the document
)

// WriteOp is a single write operation for batch/transaction commits.
type WriteOp struct {
    Type WriteOpType
    Doc  *Document // set for WriteOpUpdate; Doc.Path is the document path
    Path string    // set for WriteOpDelete; the full Firestore document path
    Mode WriteMode // ignored for WriteOpDelete
}

// WriteResult is the per-write result returned by CommitTransaction and BatchWrite.
type WriteResult struct {
    UpdatedAt time.Time
}

// CommitResult holds the results of a CommitTransaction call.
type CommitResult struct {
    WriteResults []WriteResult
    CommitTime   time.Time
}
```

- [ ] **Step 3: Add new methods to `internal/store/adapter.go`**

Append to the `StorageAdapter` interface (after `ListDocuments`):

```go
    // QueryDocuments returns documents matching q. Filters, ordering, and cursors
    // are applied; results are paginated using q.PageSize and q.PageToken.
    QueryDocuments(ctx context.Context, q *Query) (*ListPage, error)

    // WithTransaction executes fn inside a SQL transaction.
    // If fn returns an error the transaction is rolled back; otherwise committed.
    // Used for atomic BatchWrite and Commit-without-transaction-ID operations.
    WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error

    // BeginTransaction records a new Firestore transaction and returns its ID.
    // readOnly controls whether the transaction allows writes at commit time.
    BeginTransaction(ctx context.Context, readOnly bool) (txID string, err error)

    // GetDocumentForTransaction fetches a document and records the read (path +
    // current version) in the transaction's read set for OCC validation at commit.
    // Returns codes.NotFound if the document is absent.
    GetDocumentForTransaction(ctx context.Context, txID, path string) (*Document, error)

    // CommitTransaction verifies that no document in the transaction's read set
    // has been modified since it was read (codes.Aborted if violated), then
    // applies ops atomically, and deletes the transaction record.
    CommitTransaction(ctx context.Context, txID string, ops []WriteOp) (*CommitResult, error)

    // RollbackTransaction deletes the transaction record without applying any writes.
    RollbackTransaction(ctx context.Context, txID string) error
```

- [ ] **Step 4: Run build (tests will fail until adapters implement new methods)**

```
go build ./... 2>&1
```
Expected: compile errors in sqlite and postgres packages — "does not implement StorageAdapter". This is expected.

- [ ] **Step 5: Commit partial**

```bash
git add internal/store/doc.go internal/store/adapter.go internal/store/adapter_test.go
git commit -m "feat(store): add query/transaction/batchwrite types to adapter interface"
```

---

## Task 4: SQLite — QueryDocuments + transaction methods

**Files:**
- Modify: `internal/store/sqlite/sqlite.go`
- Test: `internal/store/sqlite/sqlite_test.go`

- [ ] **Step 1: Write failing tests**

Add to `internal/store/sqlite/sqlite_test.go`:

```go
func TestQueryDocuments_StringFilter(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    _ = mustCreate(t, a, "projects/p/databases/d/documents/items/a",
        `{"fields":{"status":{"stringValue":"active"},"name":{"stringValue":"alpha"}}}`)
    _ = mustCreate(t, a, "projects/p/databases/d/documents/items/b",
        `{"fields":{"status":{"stringValue":"inactive"},"name":{"stringValue":"beta"}}}`)
    _ = mustCreate(t, a, "projects/p/databases/d/documents/items/c",
        `{"fields":{"status":{"stringValue":"active"},"name":{"stringValue":"gamma"}}}`)

    q := &store.Query{
        Parent:       "projects/p/databases/d/documents",
        CollectionID: "items",
        Filter: &store.CompositeFilter{Filters: []store.FieldFilter{
            {Field: "status", Op: store.FilterOpEqual,
                Value: store.FilterValue{Kind: store.FilterValueString, StrVal: "active"}},
        }},
        PageSize: 100,
    }
    page, err := a.QueryDocuments(ctx, q)
    require.NoError(t, err)
    require.Len(t, page.Documents, 2)
}

func TestQueryDocuments_IntFilter(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    _ = mustCreate(t, a, "projects/p/databases/d/documents/things/x",
        `{"fields":{"score":{"integerValue":"10"}}}`)
    _ = mustCreate(t, a, "projects/p/databases/d/documents/things/y",
        `{"fields":{"score":{"integerValue":"20"}}}`)
    _ = mustCreate(t, a, "projects/p/databases/d/documents/things/z",
        `{"fields":{"score":{"integerValue":"5"}}}`)

    q := &store.Query{
        Parent:       "projects/p/databases/d/documents",
        CollectionID: "things",
        Filter: &store.CompositeFilter{Filters: []store.FieldFilter{
            {Field: "score", Op: store.FilterOpGreaterThan,
                Value: store.FilterValue{Kind: store.FilterValueInt, IntVal: 9}},
        }},
        OrderBy:  []store.OrderBy{{Field: "score", Direction: store.DirectionAsc}},
        PageSize: 100,
    }
    page, err := a.QueryDocuments(ctx, q)
    require.NoError(t, err)
    require.Len(t, page.Documents, 2)
}

func TestBeginCommitTransaction(t *testing.T) {
    a := newTestAdapter(t)
    ctx := context.Background()

    _ = mustCreate(t, a, "projects/p/databases/d/documents/col/doc1",
        `{"fields":{"val":{"integerValue":"1"}}}`)

    txID, err := a.BeginTransaction(ctx, false)
    require.NoError(t, err)
    require.NotEmpty(t, txID)

    doc, err := a.GetDocumentForTransaction(ctx, txID, "projects/p/databases/d/documents/col/doc1")
    require.NoError(t, err)
    require.NotNil(t, doc)

    // Update the document outside the transaction — this should cause Aborted
    _, err = a.UpdateDocument(ctx, &store.Document{
        Path: "projects/p/databases/d/documents/col/doc1",
        Data: `{"fields":{"val":{"integerValue":"99"}}}`,
    }, store.WriteModeUpdate)
    require.NoError(t, err)

    // Commit should be aborted because the read version no longer matches
    newDoc := &store.Document{
        Path: "projects/p/databases/d/documents/col/doc1",
        Data: `{"fields":{"val":{"integerValue":"2"}}}`,
    }
    _, err = a.CommitTransaction(ctx, txID, []store.WriteOp{
        {Type: store.WriteOpUpdate, Doc: newDoc, Mode: store.WriteModeUpdate},
    })
    require.Error(t, err)
    require.Equal(t, codes.Aborted, status.Code(err))
}

// mustCreate is a test helper (add once at the top of sqlite_test.go if not present):
func mustCreate(t *testing.T, a store.StorageAdapter, path, data string) *store.Document {
    t.Helper()
    d, err := a.CreateDocument(context.Background(), &store.Document{Path: path, Data: data})
    require.NoError(t, err)
    return d
}
```

- [ ] **Step 2: Run tests, verify they fail**

```
go test ./internal/store/sqlite/... -run TestQuery -run TestBeginCommit -v
```
Expected: FAIL — methods not implemented.

- [ ] **Step 3: Add query helpers to `internal/store/sqlite/sqlite.go`**

Append to `internal/store/sqlite/sqlite.go`:

```go
// ─── Query helpers ────────────────────────────────────────────────────────────

// sqliteFieldBase returns the $.fields.<path> JSON path without a type suffix.
// For "name" → "$.fields.name"; for "a.b" → "$.fields.a.mapValue.fields.b".
func sqliteFieldBase(fieldPath string) string {
    parts := strings.Split(fieldPath, ".")
    var sb strings.Builder
    sb.WriteString("$.fields.")
    for i, p := range parts {
        sb.WriteString(p)
        if i < len(parts)-1 {
            sb.WriteString(".mapValue.fields.")
        }
    }
    return sb.String()
}

// sqliteTypedPath appends the protojson type key for the given kind.
func sqliteTypedPath(base string, kind store.FilterValueKind) string {
    switch kind {
    case store.FilterValueString: return base + ".stringValue"
    case store.FilterValueInt:    return base + ".integerValue"
    case store.FilterValueDouble: return base + ".doubleValue"
    case store.FilterValueBool:   return base + ".booleanValue"
    case store.FilterValueTime:   return base + ".timestampValue"
    default: return base
    }
}

// sqliteScalarExpr returns the SQL expression and value to use for a scalar filter.
// Returns expression like `json_extract(data,'$.fields.x.stringValue')` and the
// SQL argument value to bind. Numeric fields use CAST(... AS REAL).
func sqliteScalarExpr(fieldPath string, fv store.FilterValue) (expr string, arg interface{}) {
    base := sqliteFieldBase(fieldPath)
    path := sqliteTypedPath(base, fv.Kind)
    switch fv.Kind {
    case store.FilterValueInt:
        return fmt.Sprintf("CAST(json_extract(data,'%s') AS REAL)", path), float64(fv.IntVal)
    case store.FilterValueDouble:
        return fmt.Sprintf("CAST(json_extract(data,'%s') AS REAL)", path), fv.DoubleVal
    case store.FilterValueBool:
        b := 0
        if fv.BoolVal { b = 1 }
        return fmt.Sprintf("json_extract(data,'%s')", path), b
    case store.FilterValueTime:
        return fmt.Sprintf("json_extract(data,'%s')", path), fv.TimeVal.UTC().Format(time.RFC3339Nano)
    default:
        return fmt.Sprintf("json_extract(data,'%s')", path), fv.StrVal
    }
}

// sqliteFilterClause builds a SQL WHERE clause fragment for a single FieldFilter.
func sqliteFilterClause(f store.FieldFilter, args *[]interface{}) (string, error) {
    switch f.Op {
    case store.FilterOpEqual, store.FilterOpNotEqual,
         store.FilterOpLessThan, store.FilterOpLessThanOrEqual,
         store.FilterOpGreaterThan, store.FilterOpGreaterThanOrEqual:
        expr, arg := sqliteScalarExpr(f.Field, f.Value)
        *args = append(*args, arg)
        opMap := map[store.FilterOp]string{
            store.FilterOpEqual: "=", store.FilterOpNotEqual: "!=",
            store.FilterOpLessThan: "<", store.FilterOpLessThanOrEqual: "<=",
            store.FilterOpGreaterThan: ">", store.FilterOpGreaterThanOrEqual: ">=",
        }
        return fmt.Sprintf("%s %s ?", expr, opMap[f.Op]), nil

    case store.FilterOpIn, store.FilterOpNotIn:
        base := sqliteFieldBase(f.Field)
        phs := make([]string, len(f.Value.ArrayVals))
        for i, v := range f.Value.ArrayVals {
            phs[i] = "?"
            _, arg := sqliteScalarExpr(f.Field, v)
            _ = base // use base to derive the path
            path := sqliteTypedPath(base, v.Kind)
            _ = path
            *args = append(*args, arg)
        }
        op := "IN"
        if f.Op == store.FilterOpNotIn { op = "NOT IN" }
        path := sqliteTypedPath(base, f.Value.Kind)
        if len(f.Value.ArrayVals) > 0 { path = sqliteTypedPath(base, f.Value.ArrayVals[0].Kind) }
        return fmt.Sprintf("json_extract(data,'%s') %s (%s)",
            path, op, strings.Join(phs, ",")), nil

    case store.FilterOpArrayContains:
        base := sqliteFieldBase(f.Field)
        arrayPath := base + ".arrayValue.values"
        _, arg := sqliteScalarExpr(f.Field, f.Value)
        elemPath := "'$." + strings.TrimPrefix(sqliteTypedPath("", f.Value.Kind), ".") + "'"
        _ = elemPath
        typeKey := ""
        switch f.Value.Kind {
        case store.FilterValueString: typeKey = "$.stringValue"
        case store.FilterValueInt:    typeKey = "$.integerValue"
        case store.FilterValueDouble: typeKey = "$.doubleValue"
        case store.FilterValueBool:   typeKey = "$.booleanValue"
        }
        *args = append(*args, arg)
        return fmt.Sprintf(
            "EXISTS (SELECT 1 FROM json_each(json_extract(data,'%s')) AS e WHERE json_extract(e.value,'%s') = ?)",
            arrayPath, typeKey), nil

    default:
        return "", status.Errorf(codes.Unimplemented, "filter op %v not supported in sqlite", f.Op)
    }
}

// sqliteOrderExpr builds an ORDER BY expression for a single OrderBy field.
func sqliteOrderExpr(o store.OrderBy) string {
    base := sqliteFieldBase(o.Field)
    return fmt.Sprintf("json_extract(data,'%s') %s", base, string(o.Direction))
}
```

- [ ] **Step 4: Add `QueryDocuments` to `internal/store/sqlite/sqlite.go`**

```go
// QueryDocuments returns documents matching q with filters, ordering, and pagination.
func (a *Adapter) QueryDocuments(ctx context.Context, q *store.Query) (*store.ListPage, error) {
    pageSize := q.PageSize
    if pageSize <= 0 {
        pageSize = 300
    }
    offset := store.DecodePageToken(q.PageToken)
    fetchSize := int(pageSize) + 1

    var sb strings.Builder
    args := make([]interface{}, 0, 8)

    sb.WriteString(`SELECT path, data, created_at, updated_at, version FROM documents WHERE parent = ?`)
    args = append(args, q.Parent)

    if q.CollectionID != "" {
        sb.WriteString(` AND collection = ?`)
        args = append(args, q.CollectionID)
    }

    if q.Filter != nil {
        for _, f := range q.Filter.Filters {
            clause, err := sqliteFilterClause(f, &args)
            if err != nil {
                return nil, err
            }
            sb.WriteString(" AND ")
            sb.WriteString(clause)
        }
    }

    if len(q.OrderBy) > 0 {
        sb.WriteString(" ORDER BY ")
        for i, o := range q.OrderBy {
            if i > 0 { sb.WriteString(", ") }
            sb.WriteString(sqliteOrderExpr(o))
        }
    } else {
        sb.WriteString(" ORDER BY path ASC")
    }

    sb.WriteString(fmt.Sprintf(" LIMIT %d OFFSET %d", fetchSize, offset))

    rows, err := a.db.QueryContext(ctx, sb.String(), args...)
    if err != nil {
        return nil, fmt.Errorf("sqlite: query documents: %w", err)
    }
    defer rows.Close()

    docs := make([]*store.Document, 0, pageSize)
    for rows.Next() {
        d, err := scanDoc(rows)
        if err != nil {
            return nil, fmt.Errorf("sqlite: scan query row: %w", err)
        }
        docs = append(docs, d)
    }
    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("sqlite: query rows: %w", err)
    }

    var nextToken string
    if len(docs) > int(pageSize) {
        docs = docs[:pageSize]
        nextToken = store.EncodePageToken(offset + int(pageSize))
    }
    return &store.ListPage{Documents: docs, NextPageToken: nextToken}, nil
}
```

- [ ] **Step 5: Add `WithTransaction` to `internal/store/sqlite/sqlite.go`**

```go
// WithTransaction executes fn inside a SQLite transaction.
func (a *Adapter) WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
    tx, err := a.db.BeginTx(ctx, nil)
    if err != nil {
        return fmt.Errorf("sqlite: begin tx: %w", err)
    }
    // Swap the adapter's db handle for the tx handle inside fn using a txAdapter wrapper.
    txA := &txAdapter{Adapter: a, tx: tx}
    if err := fn(context.WithValue(ctx, txAdapterKey{}, txA)); err != nil {
        _ = tx.Rollback()
        return err
    }
    return tx.Commit()
}

type txAdapterKey struct{}

// txAdapter wraps Adapter and redirects CRUD calls to an open sql.Tx.
// Only the methods needed for WithTransaction (CreateDocument, UpdateDocument,
// DeleteDocument, GetDocument) are overridden; all others fall through to the base.
type txAdapter struct {
    *Adapter
    tx *sql.Tx
}
```

Note: For the `txAdapter` to override CRUD methods using the `tx` instead of `a.db`, each CRUD method
needs to accept a `*sql.DB`-or-`*sql.Tx` interface. The simplest approach is to introduce a helper
`sqlExecer` interface:

```go
type sqlExecer interface {
    QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
    ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
    QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}
```

Store the `db` field as `sqlExecer` in `Adapter`. `*sql.DB` and `*sql.Tx` both satisfy this.
Change `a.db` from `*sql.DB` to `sqlExecer`. The `New` constructor and `Migrate`/`Ping`/`Close`
methods that need `*sql.DB` specifically should store a second `rawDB *sql.DB` field:

```go
type Adapter struct {
    rawDB          *sql.DB    // for Ping, Migrate, Close, WAL mode, TXs
    db             sqlExecer  // for CRUD — replaced by sql.Tx inside WithTransaction
    migrationsPath string
}
```

Update `New` to set both fields. The `WithTransaction` creates a `txAdapter` with `db = tx`.

- [ ] **Step 6: Add `BeginTransaction`, `GetDocumentForTransaction`, `CommitTransaction`, `RollbackTransaction`**

```go
import (
    "encoding/hex"
    "crypto/rand"
    "encoding/json"
)

func newTxID() string {
    b := make([]byte, 16)
    _, _ = rand.Read(b)
    return hex.EncodeToString(b)
}

func (a *Adapter) BeginTransaction(ctx context.Context, readOnly bool) (string, error) {
    id := newTxID()
    now := time.Now().UTC().Format(time.RFC3339Nano)
    expires := time.Now().UTC().Add(60 * time.Second).Format(time.RFC3339Nano)
    _, err := a.db.ExecContext(ctx,
        `INSERT INTO transactions (id, started_at, reads, expires_at) VALUES (?, ?, '{}', ?)`,
        id, now, expires)
    if err != nil {
        return "", fmt.Errorf("sqlite: begin transaction: %w", err)
    }
    return id, nil
}

func (a *Adapter) GetDocumentForTransaction(ctx context.Context, txID, path string) (*store.Document, error) {
    doc, err := a.GetDocument(ctx, path)
    if err != nil {
        return nil, err // propagates codes.NotFound
    }
    // Record the read: merge {path: version} into transactions.reads JSON.
    _, err = a.db.ExecContext(ctx,
        `UPDATE transactions SET reads = json_set(reads, '$.' || ?, ?) WHERE id = ?`,
        path, doc.Version, txID)
    if err != nil {
        return nil, fmt.Errorf("sqlite: record transaction read: %w", err)
    }
    return doc, nil
}

func (a *Adapter) CommitTransaction(ctx context.Context, txID string, ops []store.WriteOp) (*store.CommitResult, error) {
    // Load transaction read set.
    var readsJSON string
    err := a.rawDB.QueryRowContext(ctx,
        `SELECT reads FROM transactions WHERE id = ?`, txID).Scan(&readsJSON)
    if err != nil {
        if errors.Is(err, sql.ErrNoRows) {
            return nil, status.Errorf(codes.NotFound, "sqlite: transaction not found: %s", txID)
        }
        return nil, fmt.Errorf("sqlite: load transaction: %w", err)
    }
    var reads map[string]int64
    if err := json.Unmarshal([]byte(readsJSON), &reads); err != nil {
        return nil, fmt.Errorf("sqlite: decode transaction reads: %w", err)
    }

    now := time.Now().UTC()

    // Execute OCC check + writes in a SQL transaction.
    tx, err := a.rawDB.BeginTx(ctx, nil)
    if err != nil {
        return nil, fmt.Errorf("sqlite: begin commit tx: %w", err)
    }

    // OCC: verify all read documents have the same version.
    for path, version := range reads {
        var current int64
        err := tx.QueryRowContext(ctx, `SELECT version FROM documents WHERE path = ?`, path).Scan(&current)
        if errors.Is(err, sql.ErrNoRows) {
            // Document was deleted since we read it — that's an OCC conflict.
            _ = tx.Rollback()
            return nil, status.Errorf(codes.Aborted, "sqlite: transaction aborted: %s was deleted", path)
        }
        if err != nil {
            _ = tx.Rollback()
            return nil, fmt.Errorf("sqlite: occ check: %w", err)
        }
        if current != version {
            _ = tx.Rollback()
            return nil, status.Errorf(codes.Aborted, "sqlite: transaction aborted: %s version mismatch (read %d, current %d)", path, version, current)
        }
    }

    // Apply writes.
    results := make([]store.WriteResult, 0, len(ops))
    for _, op := range ops {
        switch op.Type {
        case store.WriteOpUpdate:
            d := op.Doc
            if d.UpdatedAt.IsZero() { d.UpdatedAt = now }
            if d.CreatedAt.IsZero() { d.CreatedAt = now }
            collection, parent := sqliteParseCollection(d.Path)
            updAt := d.UpdatedAt.UTC().Format(time.RFC3339Nano)
            crAt := d.CreatedAt.UTC().Format(time.RFC3339Nano)
            switch op.Mode {
            case store.WriteModeUpsert:
                _, err = tx.ExecContext(ctx, `
                    INSERT INTO documents (path,collection,parent,data,created_at,updated_at,version)
                    VALUES (?,?,?,?,?,?,1)
                    ON CONFLICT(path) DO UPDATE SET
                        data=excluded.data, updated_at=excluded.updated_at,
                        version=documents.version+1`,
                    d.Path, collection, parent, d.Data, crAt, updAt)
            case store.WriteModeUpdate:
                _, err = tx.ExecContext(ctx,
                    `UPDATE documents SET data=?,updated_at=?,version=version+1 WHERE path=?`,
                    d.Data, updAt, d.Path)
            case store.WriteModeInsertOnly:
                _, err = tx.ExecContext(ctx, `
                    INSERT INTO documents (path,collection,parent,data,created_at,updated_at,version)
                    VALUES (?,?,?,?,?,?,1)`,
                    d.Path, collection, parent, d.Data, crAt, updAt)
            }
            if err != nil {
                _ = tx.Rollback()
                return nil, fmt.Errorf("sqlite: commit write: %w", err)
            }
            results = append(results, store.WriteResult{UpdatedAt: now})

        case store.WriteOpDelete:
            if _, err := tx.ExecContext(ctx, `DELETE FROM documents WHERE path=?`, op.Path); err != nil {
                _ = tx.Rollback()
                return nil, fmt.Errorf("sqlite: commit delete: %w", err)
            }
            results = append(results, store.WriteResult{UpdatedAt: now})
        }
    }

    // Delete transaction record.
    if _, err := tx.ExecContext(ctx, `DELETE FROM transactions WHERE id=?`, txID); err != nil {
        _ = tx.Rollback()
        return nil, fmt.Errorf("sqlite: delete transaction: %w", err)
    }

    if err := tx.Commit(); err != nil {
        return nil, fmt.Errorf("sqlite: commit transaction tx: %w", err)
    }
    return &store.CommitResult{WriteResults: results, CommitTime: now}, nil
}

func (a *Adapter) RollbackTransaction(ctx context.Context, txID string) error {
    _, err := a.db.ExecContext(ctx, `DELETE FROM transactions WHERE id=?`, txID)
    return err
}
```

- [ ] **Step 7: Run tests, verify they pass**

```
go test ./internal/store/sqlite/... -run TestQuery -run TestBeginCommit -v
```
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/store/sqlite/sqlite.go internal/store/sqlite/sqlite_test.go
git commit -m "feat(sqlite): QueryDocuments, BeginTransaction, CommitTransaction, RollbackTransaction"
```

---

## Task 5: PostgreSQL — QueryDocuments + transaction methods

**Files:**
- Modify: `internal/store/postgres/postgres.go`
- Test: `internal/store/postgres/postgres_test.go`

- [ ] **Step 1: Write failing tests**

Add the same tests as Task 4 Step 1 to `internal/store/postgres/postgres_test.go` (replace `newTestAdapter` with the postgres-specific constructor). The tests are structurally identical; only the adapter constructor differs.

- [ ] **Step 2: Run tests, verify they fail**

```
go test ./internal/store/postgres/... -run TestQuery -run TestBeginCommit -v
```
Expected: FAIL — methods not implemented.

- [ ] **Step 3: Add PostgreSQL query helpers to `internal/store/postgres/postgres.go`**

```go
// ─── Query helpers ─────────────────────────────────────────────────────────

// pgFieldBase returns the JSONB navigation expression to the field's typed value node.
// "name" → `data->'fields'->'name'`
// "a.b"  → `data->'fields'->'a'->'mapValue'->'fields'->'b'`
func pgFieldBase(fieldPath string) string {
    parts := strings.Split(fieldPath, ".")
    var sb strings.Builder
    sb.WriteString("data->'fields'")
    for i, p := range parts {
        sb.WriteString(fmt.Sprintf("->'%s'", p))
        if i < len(parts)-1 {
            sb.WriteString("->'mapValue'->'fields'")
        }
    }
    return sb.String()
}

// pgTypedExpr returns the JSONB expression that extracts a scalar value as text
// and, for numerics, the expression for casting.
// Returns (cast_expr, text_expr) where cast_expr is used for comparisons.
func pgScalarExpr(fieldPath string, kind store.FilterValueKind) string {
    base := pgFieldBase(fieldPath)
    switch kind {
    case store.FilterValueString: return base + "->>'stringValue'"
    case store.FilterValueBool:   return base + "->>'booleanValue'"
    case store.FilterValueTime:   return base + "->>'timestampValue'"
    case store.FilterValueInt:
        return fmt.Sprintf("(%s->>'integerValue')::bigint", base)
    case store.FilterValueDouble:
        return fmt.Sprintf("(%s->>'doubleValue')::double precision", base)
    default:
        return base + "->>'stringValue'"
    }
}

func pgScalarArg(fv store.FilterValue) interface{} {
    switch fv.Kind {
    case store.FilterValueInt:    return fv.IntVal
    case store.FilterValueDouble: return fv.DoubleVal
    case store.FilterValueBool:
        if fv.BoolVal { return "true" }
        return "false"
    case store.FilterValueTime:   return fv.TimeVal.UTC().Format(time.RFC3339Nano)
    default:                      return fv.StrVal
    }
}

// pgFilterClause builds a PostgreSQL WHERE clause fragment.
// argN is the current $N counter; it is incremented per bound arg.
func pgFilterClause(f store.FieldFilter, argN *int, args *[]interface{}) (string, error) {
    ph := func() string {
        *argN++
        return fmt.Sprintf("$%d", *argN)
    }
    switch f.Op {
    case store.FilterOpEqual, store.FilterOpNotEqual,
         store.FilterOpLessThan, store.FilterOpLessThanOrEqual,
         store.FilterOpGreaterThan, store.FilterOpGreaterThanOrEqual:
        expr := pgScalarExpr(f.Field, f.Value.Kind)
        opMap := map[store.FilterOp]string{
            store.FilterOpEqual: "=", store.FilterOpNotEqual: "<>",
            store.FilterOpLessThan: "<", store.FilterOpLessThanOrEqual: "<=",
            store.FilterOpGreaterThan: ">", store.FilterOpGreaterThanOrEqual: ">=",
        }
        *args = append(*args, pgScalarArg(f.Value))
        return fmt.Sprintf("%s %s %s", expr, opMap[f.Op], ph()), nil

    case store.FilterOpIn, store.FilterOpNotIn:
        expr := pgScalarExpr(f.Field, f.Value.Kind)
        phs := make([]string, len(f.Value.ArrayVals))
        for i, v := range f.Value.ArrayVals {
            *args = append(*args, pgScalarArg(v))
            phs[i] = ph()
        }
        op := "IN"
        if f.Op == store.FilterOpNotIn { op = "NOT IN" }
        return fmt.Sprintf("%s %s (%s)", expr, op, strings.Join(phs, ",")), nil

    case store.FilterOpArrayContains:
        base := pgFieldBase(f.Field)
        arrayExpr := base + "->'arrayValue'->'values'"
        typeKey := map[store.FilterValueKind]string{
            store.FilterValueString: "stringValue",
            store.FilterValueInt:    "integerValue",
            store.FilterValueDouble: "doubleValue",
            store.FilterValueBool:   "booleanValue",
        }[f.Value.Kind]
        *args = append(*args, pgScalarArg(f.Value))
        return fmt.Sprintf(
            "EXISTS (SELECT 1 FROM jsonb_array_elements(%s) AS elem WHERE elem->>'%s' = %s)",
            arrayExpr, typeKey, ph()), nil

    default:
        return "", status.Errorf(codes.Unimplemented, "filter op %v not supported in postgres", f.Op)
    }
}

func pgOrderExpr(o store.OrderBy) string {
    base := pgFieldBase(o.Field)
    return fmt.Sprintf("%s %s", base, string(o.Direction))
}
```

- [ ] **Step 4: Add `QueryDocuments` to postgres adapter**

```go
func (a *Adapter) QueryDocuments(ctx context.Context, q *store.Query) (*store.ListPage, error) {
    pageSize := q.PageSize
    if pageSize <= 0 { pageSize = 300 }
    offset := store.DecodePageToken(q.PageToken)
    fetchSize := int(pageSize) + 1

    var sb strings.Builder
    args := make([]interface{}, 0, 8)
    argN := 0
    ph := func() string { argN++; return fmt.Sprintf("$%d", argN) }

    args = append(args, q.Parent)
    sb.WriteString(`SELECT path, data, created_at, updated_at, version FROM documents WHERE parent = ` + ph())

    if q.CollectionID != "" {
        args = append(args, q.CollectionID)
        sb.WriteString(` AND collection = ` + ph())
    }

    if q.Filter != nil {
        for _, f := range q.Filter.Filters {
            clause, err := pgFilterClause(f, &argN, &args)
            if err != nil { return nil, err }
            sb.WriteString(" AND ")
            sb.WriteString(clause)
        }
    }

    if len(q.OrderBy) > 0 {
        sb.WriteString(" ORDER BY ")
        for i, o := range q.OrderBy {
            if i > 0 { sb.WriteString(", ") }
            sb.WriteString(pgOrderExpr(o))
        }
    } else {
        sb.WriteString(" ORDER BY path ASC")
    }

    args = append(args, fetchSize, offset)
    sb.WriteString(fmt.Sprintf(" LIMIT %s OFFSET %s", ph(), ph()))

    rows, err := a.db.QueryContext(ctx, sb.String(), args...)
    if err != nil { return nil, fmt.Errorf("postgres: query documents: %w", err) }
    defer rows.Close()

    docs := make([]*store.Document, 0, pageSize)
    for rows.Next() {
        d, err := scanPgDoc(rows)
        if err != nil { return nil, fmt.Errorf("postgres: scan query row: %w", err) }
        docs = append(docs, d)
    }
    if err := rows.Err(); err != nil { return nil, fmt.Errorf("postgres: query rows: %w", err) }

    var nextToken string
    if len(docs) > int(pageSize) {
        docs = docs[:pageSize]
        nextToken = store.EncodePageToken(offset + int(pageSize))
    }
    return &store.ListPage{Documents: docs, NextPageToken: nextToken}, nil
}
```

- [ ] **Step 5: Add `WithTransaction`, `BeginTransaction`, `GetDocumentForTransaction`, `CommitTransaction`, `RollbackTransaction`**

The pattern mirrors the SQLite implementation: add a `rawDB *sql.DB` field, change `db` to `sqlExecer`, use PostgreSQL `$N` placeholders.

```go
type sqlExecer interface {
    QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
    ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
    QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type Adapter struct {
    rawDB          *sql.DB
    db             sqlExecer
    migrationsPath string
}
```

`BeginTransaction`, `CommitTransaction`, `RollbackTransaction`, `GetDocumentForTransaction` follow the same logic as SQLite Task 4 Step 6 but with:
- `$1`…`$N` placeholders instead of `?`
- `json_set` → PostgreSQL's `jsonb_set`: `jsonb_set(reads::jsonb, array[$1], to_jsonb($2::bigint))` in the UPDATE
- `scanPgDoc` instead of `scanDoc`
- Time values as `time.Time` directly (not formatted strings)

For `json_set` in PostgreSQL to update the `reads` JSONB field, use:
```sql
UPDATE transactions SET reads = reads || jsonb_build_object($1::text, $2::bigint) WHERE id = $3
```

The `||` operator merges JSONB objects, adding or overwriting keys.

- [ ] **Step 6: Run tests, verify they pass**

```
go test ./internal/store/postgres/... -run TestQuery -run TestBeginCommit -v
```
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/store/postgres/postgres.go internal/store/postgres/postgres_test.go
git commit -m "feat(postgres): QueryDocuments, BeginTransaction, CommitTransaction, RollbackTransaction"
```

---

## Task 6: Update RunQuery handler to use QueryDocuments

**Files:**
- Modify: `internal/server/handlers.go`
- Test: `internal/server/handlers_test.go`

- [ ] **Step 1: Write failing test**

Add to `internal/server/handlers_test.go`:

```go
func TestRunQuery_WithFilter(t *testing.T) {
    srv, _ := startTestServer(t)
    ctx := context.Background()
    restBase := srv.restBase

    // Create documents
    createDoc(t, restBase, "projects/p/databases/(default)/documents", "items", map[string]interface{}{
        "fields": map[string]interface{}{"status": map[string]interface{}{"stringValue": "active"}},
    })
    createDoc(t, restBase, "projects/p/databases/(default)/documents", "items", map[string]interface{}{
        "fields": map[string]interface{}{"status": map[string]interface{}{"stringValue": "inactive"}},
    })

    // Run a filtered query via POST :runQuery
    body := `{
        "structuredQuery": {
            "from": [{"collectionId": "items"}],
            "where": {
                "fieldFilter": {
                    "field": {"fieldPath": "status"},
                    "op": "EQUAL",
                    "value": {"stringValue": "active"}
                }
            }
        }
    }`
    resp, err := http.Post(
        restBase+"/v1/projects/p/databases/(default)/documents:runQuery",
        "application/json", strings.NewReader(body))
    require.NoError(t, err)
    defer resp.Body.Close()
    require.Equal(t, http.StatusOK, resp.StatusCode)

    var results []map[string]interface{}
    require.NoError(t, json.NewDecoder(resp.Body).Decode(&results))
    // Filter out the terminal {"done":true} entry
    docs := 0
    for _, r := range results {
        if r["document"] != nil { docs++ }
    }
    require.Equal(t, 1, docs, "expected 1 active document")
}
```

- [ ] **Step 2: Run test, verify it fails**

```
go test ./internal/server/... -run TestRunQuery_WithFilter -v
```
Expected: FAIL — RunQuery returns all documents, ignoring the filter.

- [ ] **Step 3: Update `RunQuery` in `internal/server/handlers.go`**

Replace the existing `RunQuery` implementation with one that calls `QueryDocuments` when the query has filters or ordering, and falls back to `ListDocuments` for plain collection scans:

```go
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
```

Add `"github.com/petereon/embyr/internal/codec"` to the import if not already present (it should be).

- [ ] **Step 4: Run tests, verify they pass**

```
go test ./internal/server/... -run TestRunQuery -v
go test ./... 2>&1
```
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/server/handlers.go internal/server/handlers_test.go
git commit -m "feat(server): RunQuery uses QueryDocuments with full filter/orderBy support"
```

---

## Task 7: Field transforms in Commit (serverTimestamp, increment, arrayUnion, arrayRemove)

**Files:**
- Modify: `internal/server/handlers.go`
- Test: `internal/server/handlers_test.go`

Firestore field transforms arrive in `Write.update_transforms` as `[]*firestorev1.FieldTransform`.
They must be applied to the document's `Fields` map **before** encoding to store format.

- [ ] **Step 1: Write failing test**

```go
func TestCommit_ServerTimestamp(t *testing.T) {
    srv, _ := startTestServer(t)
    ctx := context.Background()

    // Create a document with serverTimestamp via Commit
    gc := srv.grpcClient(t)
    client := firestorev1.NewFirestoreClient(gc)
    defer gc.Close()

    docPath := "projects/p/databases/(default)/documents/ts/doc1"
    resp, err := client.Commit(ctx, &firestorev1.CommitRequest{
        Database: "projects/p/databases/(default)",
        Writes: []*firestorev1.Write{{
            Operation: &firestorev1.Write_Update{
                Update: &firestorev1.Document{
                    Name:   docPath,
                    Fields: map[string]*firestorev1.Value{},
                },
            },
            UpdateTransforms: []*firestorev1.DocumentTransform_FieldTransform{{
                FieldPath: "createdAt",
                TransformType: &firestorev1.DocumentTransform_FieldTransform_SetToServerValue{
                    SetToServerValue: firestorev1.DocumentTransform_FieldTransform_REQUEST_TIME,
                },
            }},
        }},
    })
    require.NoError(t, err)
    require.Len(t, resp.WriteResults, 1)

    // Verify the field was set
    doc, err := client.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: docPath})
    require.NoError(t, err)
    _, ok := doc.Fields["createdAt"]
    require.True(t, ok, "createdAt field should have been set by serverTimestamp")
    require.NotNil(t, doc.Fields["createdAt"].GetTimestampValue(), "createdAt should be a timestamp")
}
```

Note: `UpdateTransforms` field name — check the exact field name in the generated proto. It may be `UpdateTransforms` or `update_transforms` depending on codegen. Use `w.GetUpdateTransforms()` to retrieve it.

- [ ] **Step 2: Run test, verify it fails**

```
go test ./internal/server/... -run TestCommit_ServerTimestamp -v
```
Expected: FAIL — createdAt field not set.

- [ ] **Step 3: Add `applyFieldTransforms` to `internal/server/handlers.go`**

```go
// applyFieldTransforms applies field transforms to doc.Fields in place.
// The now parameter is the server-side timestamp used for REQUEST_TIME transforms.
// For INCREMENT, ARRAY_UNION, ARRAY_REMOVE a read of the current document is needed;
// currFields should be the existing fields map (nil if document doesn't exist yet).
func applyFieldTransforms(
    fields map[string]*firestorev1.Value,
    transforms []*firestorev1.DocumentTransform_FieldTransform,
    currFields map[string]*firestorev1.Value,
    now time.Time,
) (map[string]*firestorev1.Value, error) {
    if len(transforms) == 0 {
        return fields, nil
    }
    // Make a copy so we don't mutate the input proto.
    result := make(map[string]*firestorev1.Value, len(fields))
    for k, v := range fields {
        result[k] = v
    }

    for _, t := range transforms {
        // Only support top-level field paths in Plan 3.
        fp := t.GetFieldPath()
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
            curr := currFields[fp]
            delta := tt.Increment
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
                    case *firestorev1.Value_DoubleValue:  existing = ev.DoubleValue
                    case *firestorev1.Value_IntegerValue: existing = float64(ev.IntegerValue)
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
                    if proto.Equal(e, v) { found = true; break }
                }
                if !found { merged = append(merged, v) }
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
                    if proto.Equal(e, r) { remove = true; break }
                }
                if !remove { kept = append(kept, e) }
            }
            result[fp] = &firestorev1.Value{ValueType: &firestorev1.Value_ArrayValue{
                ArrayValue: &firestorev1.ArrayValue{Values: kept},
            }}
        }
    }
    return result, nil
}
```

Add `"google.golang.org/protobuf/proto"` to imports.

- [ ] **Step 4: Update `Commit` in `handlers.go` to call `applyFieldTransforms`**

In the `case *firestorev1.Write_Update:` block, after the `sd, err := codec.ProtoToStore(doc)` line, add transform application. The updated `Write_Update` case:

```go
case *firestorev1.Write_Update:
    doc := op.Update
    if doc.GetName() == "" {
        return nil, status.Error(codes.InvalidArgument, "write.update.name is required")
    }
    mode := store.WriteModeUpsert
    if mask := w.GetUpdateMask(); mask != nil && len(mask.GetFieldPaths()) > 0 {
        mode = store.WriteModeUpdate
    }

    // Apply field transforms if present.
    transforms := w.GetUpdateTransforms()
    fields := doc.GetFields()
    if len(transforms) > 0 {
        // For INCREMENT and ARRAY_ transforms we need the current fields.
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
            // If err == codes.NotFound, currFields stays nil (document doesn't exist yet)
        }
        var err error
        fields, err = applyFieldTransforms(fields, transforms, currFields, now)
        if err != nil {
            return nil, err
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
```

Move `now := time.Now().UTC()` to before the loop (it already is in the current implementation).

- [ ] **Step 5: Run tests, verify they pass**

```
go test ./internal/server/... -run TestCommit -v
go test ./... 2>&1
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/server/handlers.go internal/server/handlers_test.go
git commit -m "feat(server): field transforms in Commit (serverTimestamp, increment, arrayUnion, arrayRemove)"
```

---

## Task 8: Transaction + BatchWrite RPC handlers

**Files:**
- Create: `internal/server/transactions.go`
- Modify: `internal/server/handlers.go` (update `Commit` for transaction ID)
- Test: `internal/server/handlers_test.go`

- [ ] **Step 1: Write failing tests**

```go
func TestBeginRollback(t *testing.T) {
    srv, _ := startTestServer(t)
    ctx := context.Background()
    gc := srv.grpcClient(t)
    client := firestorev1.NewFirestoreClient(gc)
    defer gc.Close()

    beginResp, err := client.BeginTransaction(ctx, &firestorev1.BeginTransactionRequest{
        Database: "projects/p/databases/(default)",
    })
    require.NoError(t, err)
    require.NotEmpty(t, beginResp.GetTransaction())

    err = client.Rollback(ctx, &firestorev1.RollbackRequest{
        Database:    "projects/p/databases/(default)",
        Transaction: beginResp.GetTransaction(),
    })
    require.NoError(t, err)
}

func TestBatchWrite(t *testing.T) {
    srv, _ := startTestServer(t)
    ctx := context.Background()
    gc := srv.grpcClient(t)
    client := firestorev1.NewFirestoreClient(gc)
    defer gc.Close()

    resp, err := client.BatchWrite(ctx, &firestorev1.BatchWriteRequest{
        Database: "projects/p/databases/(default)",
        Writes: []*firestorev1.Write{
            {Operation: &firestorev1.Write_Update{Update: &firestorev1.Document{
                Name:   "projects/p/databases/(default)/documents/batch/a",
                Fields: map[string]*firestorev1.Value{"x": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 1}}},
            }}},
            {Operation: &firestorev1.Write_Update{Update: &firestorev1.Document{
                Name:   "projects/p/databases/(default)/documents/batch/b",
                Fields: map[string]*firestorev1.Value{"x": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 2}}},
            }}},
        },
    })
    require.NoError(t, err)
    require.Len(t, resp.GetWriteResults(), 2)
    for _, wr := range resp.GetWriteResults() {
        require.Nil(t, wr.GetStatus()) // no per-write errors
    }
}
```

- [ ] **Step 2: Run tests, verify they fail**

```
go test ./internal/server/... -run TestBeginRollback -run TestBatchWrite -v
```
Expected: FAIL — `Unimplemented`.

- [ ] **Step 3: Create `internal/server/transactions.go`**

```go
package server

import (
    "context"

    firestorev1 "github.com/petereon/embyr/gen/go/google/firestore/v1"
    "github.com/petereon/embyr/internal/codec"
    "github.com/petereon/embyr/internal/store"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
    "google.golang.org/protobuf/types/known/emptypb"
    "google.golang.org/protobuf/types/known/timestamppb"
)

// BeginTransaction creates a new server-side transaction and returns its ID.
func (s *firestoreServer) BeginTransaction(ctx context.Context, req *firestorev1.BeginTransactionRequest) (*firestorev1.BeginTransactionResponse, error) {
    readOnly := false
    if ro := req.GetOptions().GetReadOnly(); ro != nil {
        readOnly = true
    }
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

// BatchWrite applies a list of writes atomically. Unlike Commit, BatchWrite
// does not use Firestore transactions or OCC — it wraps the writes in a single
// SQL transaction and returns per-write results.
func (s *firestoreServer) BatchWrite(ctx context.Context, req *firestorev1.BatchWriteRequest) (*firestorev1.BatchWriteResponse, error) {
    now := timestamppb.Now()
    writeResults := make([]*firestorev1.WriteResult, len(req.GetWrites()))
    writeStatuses := make([]*status.Status, len(req.GetWrites()))
    _ = writeStatuses

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

    return &firestorev1.BatchWriteResponse{
        WriteResults: writeResults,
        Status:       make([]*status_pb.Status, len(writeResults)), // nil statuses = success
    }, nil
}
```

Note: `BatchWriteResponse` has a `Status` field with `[]*google.rpc.Status`. Import `status_pb "google.golang.org/genproto/googleapis/rpc/status"` or the equivalent genproto path. Check `go.mod` for the correct import path; it is likely `google.golang.org/grpc/status` via `status.Proto()`. Alternatively, use `google.golang.org/genproto/googleapis/rpc/status`. The nil-filled slice signals no per-write errors.

- [ ] **Step 4: Update `Commit` in `handlers.go` to handle transaction ID**

In `Commit`, before the write loop, check if `req.GetTransaction()` is set. If it is, use `db.CommitTransaction` instead of applying writes individually:

```go
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

    // No transaction: apply writes directly (existing logic).
    results := make([]*firestorev1.WriteResult, 0, len(req.GetWrites()))
    // ... (keep the existing write loop unchanged) ...
```

- [ ] **Step 5: Run tests, verify they pass**

```
go test ./internal/server/... -run TestBeginRollback -run TestBatchWrite -v
go test ./... 2>&1
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/server/transactions.go internal/server/handlers.go internal/server/handlers_test.go
git commit -m "feat(server): BeginTransaction, Rollback, BatchWrite, and transactional Commit"
```

---

## Task 9: Transaction TTL sweep goroutine

**Files:**
- Modify: `internal/server/server.go`
- Test: verify via `go test ./... 2>&1` (no new unit test — sweep is background infrastructure)

- [ ] **Step 1: Add `SweepExpiredTransactions` to the StorageAdapter interface**

In `internal/store/adapter.go`, append:

```go
    // SweepExpiredTransactions deletes transaction records whose expires_at is
    // in the past. Called periodically by the server to prevent unbounded growth.
    SweepExpiredTransactions(ctx context.Context) (deleted int, err error)
```

- [ ] **Step 2: Implement in SQLite**

In `internal/store/sqlite/sqlite.go`:

```go
func (a *Adapter) SweepExpiredTransactions(ctx context.Context) (int, error) {
    result, err := a.db.ExecContext(ctx,
        `DELETE FROM transactions WHERE expires_at < ?`,
        time.Now().UTC().Format(time.RFC3339Nano))
    if err != nil {
        return 0, fmt.Errorf("sqlite: sweep transactions: %w", err)
    }
    n, _ := result.RowsAffected()
    return int(n), nil
}
```

- [ ] **Step 3: Implement in PostgreSQL**

In `internal/store/postgres/postgres.go`:

```go
func (a *Adapter) SweepExpiredTransactions(ctx context.Context) (int, error) {
    result, err := a.db.ExecContext(ctx,
        `DELETE FROM transactions WHERE expires_at < NOW()`)
    if err != nil {
        return 0, fmt.Errorf("postgres: sweep transactions: %w", err)
    }
    n, _ := result.RowsAffected()
    return int(n), nil
}
```

- [ ] **Step 4: Add sweep goroutine to `Server.Run` in `internal/server/server.go`**

Inside `Run`, add a goroutine to the errgroup:

```go
    sweepInterval := 30 * time.Second
    eg.Go(func() error {
        ticker := time.NewTicker(sweepInterval)
        defer ticker.Stop()
        for {
            select {
            case <-ticker.C:
                n, err := s.db.SweepExpiredTransactions(egCtx)
                if err != nil {
                    s.log.Warn("transaction sweep failed", zap.Error(err))
                } else if n > 0 {
                    s.log.Info("swept expired transactions", zap.Int("deleted", n))
                }
            case <-egCtx.Done():
                return nil
            }
        }
    })
```

- [ ] **Step 5: Build and run all tests**

```
go build ./...
go test ./... 2>&1
```
Expected: clean build, all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/store/adapter.go internal/store/sqlite/sqlite.go \
        internal/store/postgres/postgres.go internal/server/server.go
git commit -m "feat(server): transaction TTL sweep goroutine"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] QueryDocuments with == / != / < / <= / > / >= filters → Task 4/5
- [x] IN / not-in → Task 4/5
- [x] array-contains → Task 4/5
- [x] orderBy (ASC/DESC) → Task 4/5
- [x] serverTimestamp() → Task 7
- [x] increment() → Task 7
- [x] arrayUnion() / arrayRemove() → Task 7
- [x] BeginTransaction + Rollback → Task 8
- [x] Commit with transaction ID (OCC) → Task 8
- [x] BatchWrite → Task 8
- [x] Transaction TTL sweep → Task 9
- [x] RunQuery uses filters → Task 6

**Type consistency:**
- `store.WriteOp` defined in Task 3, used in Tasks 4, 5, 8 ✓
- `store.CommitResult` / `store.WriteResult` defined in Task 3, used in Tasks 4, 5, 8 ✓
- `codec.QueryFromStructuredQuery` defined in Task 2, used in Task 6 ✓
- `applyFieldTransforms` defined in Task 7, called in updated `Commit` loop ✓
