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
