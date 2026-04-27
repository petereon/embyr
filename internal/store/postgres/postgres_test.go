package postgres_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/petereon/embyr/internal/store"
	"github.com/petereon/embyr/internal/store/postgres"
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
	coll := "pgusers_" + suffix
	for _, id := range []string{"a_" + suffix, "b_" + suffix} {
		doc := &store.Document{
			Path: fmt.Sprintf("%s/%s/%s", parent, coll, id), Data: "{}",
			CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(), Version: 1,
		}
		_, err := a.CreateDocument(ctx, doc)
		require.NoError(t, err)
	}

	page, err := a.ListDocuments(ctx, parent, coll, 10, "")
	require.NoError(t, err)
	assert.Len(t, page.Documents, 2)
}

func TestPostgresAdapter_UpdateDocument_MustExist_NotFound(t *testing.T) {
	a := newTestAdapter(t)
	ctx := context.Background()

	path := fmt.Sprintf("projects/p/databases/d/documents/col/%d", time.Now().UnixNano())
	doc := &store.Document{
		Path:      path,
		Data:      "{}",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
		Version:   1,
	}
	_, err := a.UpdateDocument(ctx, doc, store.WriteModeUpdate)
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestPostgresAdapter_UpdateDocument_InsertOnly_AlreadyExists(t *testing.T) {
	a := newTestAdapter(t)
	ctx := context.Background()

	path := fmt.Sprintf("projects/p/databases/d/documents/col/%d", time.Now().UnixNano())
	doc := &store.Document{
		Path:      path,
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

func TestPostgresAdapter_DeleteDocument(t *testing.T) {
	a := newTestAdapter(t)
	ctx := context.Background()

	path := fmt.Sprintf("projects/p/databases/d/documents/col/%d", time.Now().UnixNano())
	doc := &store.Document{
		Path:      path,
		Data:      "{}",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
		Version:   1,
	}
	_, err := a.CreateDocument(ctx, doc)
	require.NoError(t, err)

	err = a.DeleteDocument(ctx, path, true)
	require.NoError(t, err)

	_, err = a.GetDocument(ctx, path)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestPostgresAdapter_DeleteDocument_MustExist_NotFound(t *testing.T) {
	a := newTestAdapter(t)
	ctx := context.Background()

	path := fmt.Sprintf("projects/p/databases/d/documents/col/%d_missing", time.Now().UnixNano())
	err := a.DeleteDocument(ctx, path, true)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestPostgresAdapter_ListDocuments_Pagination(t *testing.T) {
	a := newTestAdapter(t)
	ctx := context.Background()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	parent := "projects/p/databases/d/documents"
	coll := "pgitems_" + suffix
	for i := 0; i < 5; i++ {
		doc := &store.Document{
			Path:      fmt.Sprintf("%s/%s/%d", parent, coll, i),
			Data:      "{}",
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
			Version:   1,
		}
		_, err := a.CreateDocument(ctx, doc)
		require.NoError(t, err)
	}

	page1, err := a.ListDocuments(ctx, parent, coll, 3, "")
	require.NoError(t, err)
	assert.Len(t, page1.Documents, 3)
	assert.NotEmpty(t, page1.NextPageToken)

	page2, err := a.ListDocuments(ctx, parent, coll, 3, page1.NextPageToken)
	require.NoError(t, err)
	assert.Len(t, page2.Documents, 2)
	assert.Empty(t, page2.NextPageToken)
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

func TestSubscribe_ReceivesChange(t *testing.T) {
	// Requires a live PostgreSQL instance. Skip if DSN not set.
	a := newTestAdapter(t)
	ch, cancel := a.Subscribe(context.Background())
	defer cancel()

	ctx := context.Background()
	_, err := a.CreateDocument(ctx, &store.Document{
		Path: "projects/p/databases/d/documents/sub/test1",
		Data: `{"fields":{"x":{"integerValue":"1"}}}`,
	})
	require.NoError(t, err)

	select {
	case change := <-ch:
		require.Equal(t, "projects/p/databases/d/documents/sub/test1", change.Path)
		require.Equal(t, store.DocChangeUpsert, change.Kind)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for change notification")
	}
}

func TestPostgresAdapter_ListDocuments_ExactPageBoundary(t *testing.T) {
	a := newTestAdapter(t)
	ctx := context.Background()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	parent := "projects/p/databases/d/documents"
	coll := "pgexact_" + suffix
	for i := 0; i < 6; i++ {
		doc := &store.Document{
			Path:      fmt.Sprintf("%s/%s/%d", parent, coll, i),
			Data:      "{}",
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
			Version:   1,
		}
		_, err := a.CreateDocument(ctx, doc)
		require.NoError(t, err)
	}

	page1, err := a.ListDocuments(ctx, parent, coll, 3, "")
	require.NoError(t, err)
	assert.Len(t, page1.Documents, 3)
	assert.NotEmpty(t, page1.NextPageToken)

	page2, err := a.ListDocuments(ctx, parent, coll, 3, page1.NextPageToken)
	require.NoError(t, err)
	assert.Len(t, page2.Documents, 3)
	assert.Empty(t, page2.NextPageToken, "exact multiple of pageSize must not emit a next token")
}

// ── Subscribe LISTEN/NOTIFY tests ─────────────────────────────────────────────
// These tests require a running PostgreSQL instance. Set TEST_POSTGRES_DSN to
// enable them, e.g.:
//   TEST_POSTGRES_DSN="postgres://postgres:postgres@localhost:5432/embyr_test?sslmode=disable" go test ./internal/store/postgres/...

func TestPostgres_Subscribe_ReceivesChange(t *testing.T) {
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
		assert.Equal(t, "projects/p/databases/d/documents/sub/doc1", change.Path)
		assert.Equal(t, store.DocChangeUpsert, change.Kind)
		assert.Equal(t, "sub", change.Collection)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for LISTEN/NOTIFY change")
	}
}

func TestPostgres_Subscribe_MultipleSubscribers(t *testing.T) {
	a := newTestAdapter(t)
	ch1, cancel1 := a.Subscribe(context.Background())
	defer cancel1()
	ch2, cancel2 := a.Subscribe(context.Background())
	defer cancel2()

	_, err := a.CreateDocument(context.Background(), &store.Document{
		Path: "projects/p/databases/d/documents/multi/doc1",
		Data: `{"fields":{"n":{"integerValue":"1"}}}`,
	})
	require.NoError(t, err)

	for i, ch := range []<-chan store.DocChange{ch1, ch2} {
		select {
		case change := <-ch:
			assert.Equal(t, "projects/p/databases/d/documents/multi/doc1", change.Path,
				"subscriber %d should receive the change", i+1)
		case <-time.After(3 * time.Second):
			t.Fatalf("subscriber %d: timeout waiting for change", i+1)
		}
	}
}

func TestPostgres_Subscribe_CancelCleanup(t *testing.T) {
	a := newTestAdapter(t)
	ch, cancel := a.Subscribe(context.Background())
	cancel() // cancel immediately

	// Channel should be closed after cancel.
	select {
	case _, open := <-ch:
		assert.False(t, open, "channel must be closed after cancel")
	case <-time.After(time.Second):
		t.Fatal("channel not closed after cancel")
	}

	// A write after cancel must not block or panic.
	_, err := a.CreateDocument(context.Background(), &store.Document{
		Path: "projects/p/databases/d/documents/postcancels/doc1",
		Data: `{}`,
	})
	require.NoError(t, err)
}

func TestPostgres_Subscribe_DeleteDelivers(t *testing.T) {
	a := newTestAdapter(t)

	// Create then delete — both events should be delivered.
	path := "projects/p/databases/d/documents/del/doc1"
	_, err := a.CreateDocument(context.Background(), &store.Document{
		Path: path,
		Data: `{"fields":{"v":{"integerValue":"1"}}}`,
	})
	require.NoError(t, err)

	ch, cancel := a.Subscribe(context.Background())
	defer cancel()

	// Drain the Create event (or start fresh by subscribing after create).
	// Subscribe after create — drain any buffered create event.
	select {
	case c := <-ch:
		if c.Path != path || c.Kind != store.DocChangeUpsert {
			t.Fatalf("unexpected first change: %+v", c)
		}
	case <-time.After(3 * time.Second):
		// Subscribe started after create; no create event expected.
	}

	require.NoError(t, a.DeleteDocument(context.Background(), path, true))

	select {
	case change := <-ch:
		assert.Equal(t, path, change.Path)
		assert.Equal(t, store.DocChangeDelete, change.Kind)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for delete change")
	}
}

func TestPostgresAdapter_OrderByInteger_NumericSort(t *testing.T) {
	a := newTestAdapter(t)
	ctx := context.Background()

	base := fmt.Sprintf("projects/p/databases/d/documents/things/%d", time.Now().UnixNano())
	for _, tc := range []struct {
		id    string
		votes string
	}{
		{"nine", "9"},
		{"ten", "10"},
		{"one", "1"},
	} {
		_, err := a.CreateDocument(ctx, &store.Document{
			Path:      base + "/" + tc.id,
			Data:      fmt.Sprintf(`{"fields":{"votes":{"integerValue":"%s"}}}`, tc.votes),
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
			Version:   1,
		})
		require.NoError(t, err)
	}

	q := &store.Query{
		Parent:       "projects/p/databases/d/documents",
		CollectionID: "things",
		OrderBy:      []store.OrderBy{{Field: "votes", Direction: store.DirectionAsc}},
		PageSize:     100,
	}
	page, err := a.QueryDocuments(ctx, q)
	require.NoError(t, err)

	var paths []string
	for _, d := range page.Documents {
		paths = append(paths, d.Path)
	}
	require.GreaterOrEqual(t, len(paths), 3)
	// Find our three docs in results (may include docs from other tests)
	var found []string
	for _, p := range paths {
		if p == base+"/one" || p == base+"/nine" || p == base+"/ten" {
			found = append(found, p)
		}
	}
	require.Len(t, found, 3, "expected to find all three docs in result")
	assert.Equal(t, base+"/one", found[0], "numeric 1 must sort before 9")
	assert.Equal(t, base+"/nine", found[1], "numeric 9 must sort before 10")
	assert.Equal(t, base+"/ten", found[2], "numeric 10 must sort last")
}
