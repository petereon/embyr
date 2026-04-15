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
