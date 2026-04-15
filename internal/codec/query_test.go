package codec_test

import (
	"testing"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"github.com/petereon/firstyr/internal/codec"
	"github.com/petereon/firstyr/internal/store"
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
