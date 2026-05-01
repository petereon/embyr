package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/petereon/embyr/internal/store"
	"github.com/stretchr/testify/require"
)

// seedDocs creates 3 docs in collection `colID` with payload built from data.
// Returns the parent path used.
func seedDocs(t *testing.T, a interface {
	CreateDocument(ctx context.Context, doc *store.Document) (*store.Document, error)
}, colID string, docs map[string]string) string {
	t.Helper()
	parent := "projects/p/databases/d/documents"
	for id, data := range docs {
		_, err := a.CreateDocument(context.Background(), &store.Document{
			Path:      parent + "/" + colID + "/" + id,
			Data:      data,
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
			Version:   1,
		})
		require.NoError(t, err)
	}
	return parent
}

// #PG-NULL — where('foo','==',null) must match docs where the field is
// explicitly null OR absent. Currently returns zero rows on Postgres.
func TestPostgresAdapter_NullEqualityFilter(t *testing.T) {
	a := newTestAdapter(t)

	colID := fmt.Sprintf("nulltest_%d", time.Now().UnixNano())
	parent := seedDocs(t, a, colID, map[string]string{
		"with_null":  `{"fields":{"foo":{"nullValue":null}}}`,
		"with_value": `{"fields":{"foo":{"stringValue":"x"}}}`,
	})

	page, err := a.QueryDocuments(context.Background(), &store.Query{
		Parent:       parent,
		CollectionID: colID,
		Filter: &store.CompositeFilter{Filters: []store.FieldFilter{{
			Field: "foo",
			Op:    store.FilterOpEqual,
			Value: store.FilterValue{Kind: store.FilterValueNull},
		}}},
		PageSize: 100,
	})
	require.NoError(t, err)
	require.Len(t, page.Documents, 1, "where('foo','==',null) must match the doc with explicit nullValue")
	require.Contains(t, page.Documents[0].Path, "with_null")
}

// #PG-CT — Cross-type compare: doc has integerValue, query is doubleValue.
// Must match (Firestore treats integer 5 == double 5.0).
func TestPostgresAdapter_CrossType_IntDoubleEqual(t *testing.T) {
	a := newTestAdapter(t)

	colID := fmt.Sprintf("crosstype_%d", time.Now().UnixNano())
	parent := seedDocs(t, a, colID, map[string]string{
		"as_int":    `{"fields":{"v":{"integerValue":"5"}}}`,
		"as_double": `{"fields":{"v":{"doubleValue":5.0}}}`,
	})

	// Filter on doubleValue=5.0 — must match BOTH the int and the double
	// representations of "five".
	page, err := a.QueryDocuments(context.Background(), &store.Query{
		Parent:       parent,
		CollectionID: colID,
		Filter: &store.CompositeFilter{Filters: []store.FieldFilter{{
			Field: "v",
			Op:    store.FilterOpEqual,
			Value: store.FilterValue{Kind: store.FilterValueDouble, DoubleVal: 5.0},
		}}},
		PageSize: 100,
	})
	require.NoError(t, err)
	require.Len(t, page.Documents, 2,
		"int-encoded 5 and double-encoded 5.0 must both match where('v','==',5.0)")
}

// #PG-ACA — array-contains-any must return docs whose array field intersects
// with the query array. Currently returns Unimplemented on Postgres.
func TestPostgresAdapter_ArrayContainsAny(t *testing.T) {
	a := newTestAdapter(t)

	colID := fmt.Sprintf("aca_%d", time.Now().UnixNano())
	parent := seedDocs(t, a, colID, map[string]string{
		"a": `{"fields":{"tags":{"arrayValue":{"values":[{"stringValue":"red"},{"stringValue":"blue"}]}}}}`,
		"b": `{"fields":{"tags":{"arrayValue":{"values":[{"stringValue":"green"}]}}}}`,
		"c": `{"fields":{"tags":{"arrayValue":{"values":[{"stringValue":"yellow"},{"stringValue":"red"}]}}}}`,
	})

	page, err := a.QueryDocuments(context.Background(), &store.Query{
		Parent:       parent,
		CollectionID: colID,
		Filter: &store.CompositeFilter{Filters: []store.FieldFilter{{
			Field: "tags",
			Op:    store.FilterOpArrayContainsAny,
			Value: store.FilterValue{Kind: store.FilterValueArray, ArrayVals: []store.FilterValue{
				{Kind: store.FilterValueString, StrVal: "red"},
				{Kind: store.FilterValueString, StrVal: "purple"},
			}},
		}}},
		PageSize: 100,
	})
	require.NoError(t, err)
	require.Len(t, page.Documents, 2,
		"array-contains-any [red,purple] must match docs containing red ('a' and 'c')")
}

// #PG-CURSOR — startAt cursor must scope query results from the given value
// onwards. Currently entirely unimplemented on Postgres.
func TestPostgresAdapter_CursorStartAt(t *testing.T) {
	a := newTestAdapter(t)

	colID := fmt.Sprintf("cursor_%d", time.Now().UnixNano())
	parent := seedDocs(t, a, colID, map[string]string{
		"d1": `{"fields":{"score":{"integerValue":"10"}}}`,
		"d2": `{"fields":{"score":{"integerValue":"20"}}}`,
		"d3": `{"fields":{"score":{"integerValue":"30"}}}`,
		"d4": `{"fields":{"score":{"integerValue":"40"}}}`,
	})

	page, err := a.QueryDocuments(context.Background(), &store.Query{
		Parent:       parent,
		CollectionID: colID,
		OrderBy:      []store.OrderBy{{Field: "score", Direction: store.DirectionAsc}},
		StartCursor: &store.Cursor{
			Values: []store.FilterValue{{Kind: store.FilterValueInt, IntVal: 25}},
			Before: true, // startAt is inclusive — but 25 is between 20 and 30, so >= 25 matches 30,40.
		},
		PageSize: 100,
	})
	require.NoError(t, err)
	require.Len(t, page.Documents, 2,
		"startAt(score=25) must return docs with score >= 25 (d3, d4); got %d", len(page.Documents))
}
