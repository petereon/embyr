package sqlite

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/petereon/embyr/internal/store"
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

func TestSQLiteAdapter_ListDocuments_ExactPageBoundary(t *testing.T) {
	a := newTestAdapter(t)
	ctx := context.Background()

	parent := "projects/p/databases/d/documents"
	for i := 0; i < 6; i++ {
		doc := &store.Document{
			Path:      fmt.Sprintf("%s/exact/%d", parent, i),
			Data:      "{}",
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
			Version:   1,
		}
		_, err := a.CreateDocument(ctx, doc)
		require.NoError(t, err)
	}

	// 6 docs with pageSize=3: two full pages, no spurious third page
	page1, err := a.ListDocuments(ctx, parent, "exact", 3, "")
	require.NoError(t, err)
	assert.Len(t, page1.Documents, 3)
	assert.NotEmpty(t, page1.NextPageToken)

	page2, err := a.ListDocuments(ctx, parent, "exact", 3, page1.NextPageToken)
	require.NoError(t, err)
	assert.Len(t, page2.Documents, 3)
	assert.Empty(t, page2.NextPageToken, "exact multiple of pageSize must not emit a next token")
}

func TestSqliteParseCollection(t *testing.T) {
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
			path:       "no_documents_segment/foo/bar",
			wantColl:   "",
			wantParent: "",
		},
		{
			path:       "projects/p/databases/d/documents/users",
			wantColl:   "",
			wantParent: "",
		},
	}
	for _, tt := range tests {
		coll, parent := sqliteParseCollection(tt.path)
		assert.Equal(t, tt.wantColl, coll, "collection for %s", tt.path)
		assert.Equal(t, tt.wantParent, parent, "parent for %s", tt.path)
	}
}

func TestSQLiteAdapter_CreateDocument_InvalidPath(t *testing.T) {
	a := newTestAdapter(t)
	ctx := context.Background()

	doc := &store.Document{
		Path:      "not/a/valid/firestore/path",
		Data:      "{}",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
		Version:   1,
	}
	_, err := a.CreateDocument(ctx, doc)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestSubscribe_ReceivesChange(t *testing.T) {
	a := newTestAdapter(t)
	ch, cancel := a.Subscribe(context.Background())
	defer cancel()

	_, err := a.CreateDocument(context.Background(), &store.Document{
		Path: "projects/p/databases/d/documents/sub/doc1",
		Data: `{"fields":{"y":{"stringValue":"hello"}}}`,
	})
	require.NoError(t, err)

	select {
	case change := <-ch:
		require.Equal(t, "projects/p/databases/d/documents/sub/doc1", change.Path)
		require.Equal(t, store.DocChangeUpsert, change.Kind)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for change")
	}
}

func mustCreate(t *testing.T, a store.StorageAdapter, path, data string) *store.Document {
	t.Helper()
	d, err := a.CreateDocument(context.Background(), &store.Document{Path: path, Data: data})
	require.NoError(t, err)
	return d
}

func TestQueryDocuments_StringFilter(t *testing.T) {
	a := newTestAdapter(t)
	ctx := context.Background()

	mustCreate(t, a, "projects/p/databases/d/documents/items/a",
		`{"fields":{"status":{"stringValue":"active"},"name":{"stringValue":"alpha"}}}`)
	mustCreate(t, a, "projects/p/databases/d/documents/items/b",
		`{"fields":{"status":{"stringValue":"inactive"},"name":{"stringValue":"beta"}}}`)
	mustCreate(t, a, "projects/p/databases/d/documents/items/c",
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

	mustCreate(t, a, "projects/p/databases/d/documents/things/x",
		`{"fields":{"score":{"integerValue":"10"}}}`)
	mustCreate(t, a, "projects/p/databases/d/documents/things/y",
		`{"fields":{"score":{"integerValue":"20"}}}`)
	mustCreate(t, a, "projects/p/databases/d/documents/things/z",
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

// TestQueryDocuments_LargeInt64Filter verifies that integers > 2^53 are compared
// correctly without float64 precision loss. 2^53 and 2^53+1 both round to the
// same float64 (2^53) — a CAST AS REAL filter would incorrectly match both.
func TestQueryDocuments_LargeInt64Filter(t *testing.T) {
	a := newTestAdapter(t)
	ctx := context.Background()

	// 2^53 = 9007199254740992 (exactly representable as float64)
	// 2^53+1 = 9007199254740993 (rounds to 2^53 in float64 — indistinguishable)
	const exact = int64(1 << 53)
	const nextInt = exact + 1
	mustCreate(t, a, "projects/p/databases/d/documents/large/a",
		fmt.Sprintf(`{"fields":{"n":{"integerValue":"%d"}}}`, exact))
	mustCreate(t, a, "projects/p/databases/d/documents/large/b",
		fmt.Sprintf(`{"fields":{"n":{"integerValue":"%d"}}}`, nextInt))

	q := &store.Query{
		Parent:       "projects/p/databases/d/documents",
		CollectionID: "large",
		Filter: &store.CompositeFilter{Filters: []store.FieldFilter{
			{Field: "n", Op: store.FilterOpEqual,
				Value: store.FilterValue{Kind: store.FilterValueInt, IntVal: exact}},
		}},
		PageSize: 100,
	}
	page, err := a.QueryDocuments(ctx, q)
	require.NoError(t, err)
	require.Len(t, page.Documents, 1, "only doc/a (n=%d) should match; doc/b (n=%d) must not", exact, nextInt)
}

func TestQueryDocuments_OrderByInteger_NumericSort(t *testing.T) {
	a := newTestAdapter(t)
	ctx := context.Background()

	mustCreate(t, a, "projects/p/databases/d/documents/things/nine",
		`{"fields":{"votes":{"integerValue":"9"}}}`)
	mustCreate(t, a, "projects/p/databases/d/documents/things/ten",
		`{"fields":{"votes":{"integerValue":"10"}}}`)
	mustCreate(t, a, "projects/p/databases/d/documents/things/one",
		`{"fields":{"votes":{"integerValue":"1"}}}`)

	q := &store.Query{
		Parent:       "projects/p/databases/d/documents",
		CollectionID: "things",
		OrderBy:      []store.OrderBy{{Field: "votes", Direction: store.DirectionAsc}},
		PageSize:     100,
	}
	page, err := a.QueryDocuments(ctx, q)
	require.NoError(t, err)
	require.Len(t, page.Documents, 3)
	// Numeric order: 1, 9, 10 — NOT lexicographic "1", "10", "9"
	assert.Contains(t, page.Documents[0].Path, "one")
	assert.Contains(t, page.Documents[1].Path, "nine")
	assert.Contains(t, page.Documents[2].Path, "ten")
}

func TestQueryDocuments_InFilter_EmptyArray(t *testing.T) {
	a := newTestAdapter(t)
	ctx := context.Background()

	mustCreate(t, a, "projects/p/databases/d/documents/things/x",
		`{"fields":{"score":{"integerValue":"5"}}}`)

	q := &store.Query{
		Parent:       "projects/p/databases/d/documents",
		CollectionID: "things",
		Filter: &store.CompositeFilter{Filters: []store.FieldFilter{
			{Field: "score", Op: store.FilterOpIn,
				Value: store.FilterValue{Kind: store.FilterValueArray, ArrayVals: nil}},
		}},
		PageSize: 100,
	}
	// Empty IN() is either an error or returns no results — must not panic/SQL error.
	page, err := a.QueryDocuments(ctx, q)
	if err == nil {
		assert.Empty(t, page.Documents)
	}
}

func TestQueryDocuments_MaliciousFieldPath_Rejected(t *testing.T) {
	a := newTestAdapter(t)
	ctx := context.Background()

	q := &store.Query{
		Parent:       "projects/p/databases/d/documents",
		CollectionID: "items",
		Filter: &store.CompositeFilter{Filters: []store.FieldFilter{
			// Single-quote in field name would break json_extract SQL string literal.
			{Field: "evil') UNION SELECT 1--", Op: store.FilterOpEqual,
				Value: store.FilterValue{Kind: store.FilterValueString, StrVal: "x"}},
		}},
		PageSize: 100,
	}
	_, err := a.QueryDocuments(ctx, q)
	require.Error(t, err, "malicious field path must be rejected")
}

func TestBeginCommitTransaction(t *testing.T) {
	a := newTestAdapter(t)
	ctx := context.Background()

	mustCreate(t, a, "projects/p/databases/d/documents/col/doc1",
		`{"fields":{"val":{"integerValue":"1"}}}`)

	txID, err := a.BeginTransaction(ctx, false)
	require.NoError(t, err)
	require.NotEmpty(t, txID)

	doc, err := a.GetDocumentForTransaction(ctx, txID, "projects/p/databases/d/documents/col/doc1")
	require.NoError(t, err)
	require.NotNil(t, doc)

	// Update the document outside the transaction — should cause Aborted
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
