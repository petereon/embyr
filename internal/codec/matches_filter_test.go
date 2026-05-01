package codec_test

import (
	"math"
	"testing"

	firestorev1 "github.com/petereon/embyr/gen/go/google/firestore/v1"
	"github.com/petereon/embyr/internal/codec"
	"github.com/petereon/embyr/internal/store"
	"github.com/stretchr/testify/require"
)

// docWith constructs a Document with a single top-level field of the given path,
// where path may use dot-notation to nest values inside MapValues.
func docWithNested(fp string, v *firestorev1.Value) *firestorev1.Document {
	// Build nested MapValues from right to left.
	segments := splitPath(fp)
	cur := v
	for i := len(segments) - 1; i >= 1; i-- {
		cur = &firestorev1.Value{ValueType: &firestorev1.Value_MapValue{
			MapValue: &firestorev1.MapValue{Fields: map[string]*firestorev1.Value{
				segments[i]: cur,
			}},
		}}
	}
	return &firestorev1.Document{Fields: map[string]*firestorev1.Value{
		segments[0]: cur,
	}}
}

func splitPath(fp string) []string {
	var out []string
	start := 0
	for i := 0; i < len(fp); i++ {
		if fp[i] == '.' {
			out = append(out, fp[start:i])
			start = i + 1
		}
	}
	out = append(out, fp[start:])
	return out
}

// #LISTEN-NESTED — MatchesFilter must descend dot-notation into MapValues.
func TestMatchesFilter_NestedField_Equal(t *testing.T) {
	doc := docWithNested("profile.age",
		&firestorev1.Value{ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 25}})

	filter := &store.CompositeFilter{Filters: []store.FieldFilter{{
		Field: "profile.age",
		Op:    store.FilterOpEqual,
		Value: store.FilterValue{Kind: store.FilterValueInt, IntVal: 25},
	}}}

	require.True(t, codec.MatchesFilter(doc, filter),
		"nested field profile.age=25 should match where('profile.age','==',25)")
}

func TestMatchesFilter_NestedField_NotMatching(t *testing.T) {
	doc := docWithNested("profile.age",
		&firestorev1.Value{ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 30}})

	filter := &store.CompositeFilter{Filters: []store.FieldFilter{{
		Field: "profile.age",
		Op:    store.FilterOpEqual,
		Value: store.FilterValue{Kind: store.FilterValueInt, IntVal: 25},
	}}}

	require.False(t, codec.MatchesFilter(doc, filter),
		"nested field profile.age=30 should not match where('profile.age','==',25)")
}

// #LISTEN-MISSING — Firestore excludes documents missing the field from any predicate,
// including != and not-in.
func TestMatchesFilter_MissingField_NotEqual_Excluded(t *testing.T) {
	doc := &firestorev1.Document{Fields: map[string]*firestorev1.Value{
		"other": {ValueType: &firestorev1.Value_StringValue{StringValue: "v"}},
	}}

	filter := &store.CompositeFilter{Filters: []store.FieldFilter{{
		Field: "missing",
		Op:    store.FilterOpNotEqual,
		Value: store.FilterValue{Kind: store.FilterValueString, StrVal: "x"},
	}}}

	require.False(t, codec.MatchesFilter(doc, filter),
		"document missing the field must NOT match != filter (Firestore excludes missing-field docs)")
}

func TestMatchesFilter_MissingField_NotIn_Excluded(t *testing.T) {
	doc := &firestorev1.Document{Fields: map[string]*firestorev1.Value{
		"other": {ValueType: &firestorev1.Value_StringValue{StringValue: "v"}},
	}}

	filter := &store.CompositeFilter{Filters: []store.FieldFilter{{
		Field: "missing",
		Op:    store.FilterOpNotIn,
		Value: store.FilterValue{Kind: store.FilterValueArray, ArrayVals: []store.FilterValue{
			{Kind: store.FilterValueString, StrVal: "x"},
		}},
	}}}

	require.False(t, codec.MatchesFilter(doc, filter),
		"document missing the field must NOT match not-in filter (Firestore excludes missing-field docs)")
}

// #LIVE-INT64 — comparing large int64 values must not lose precision via float64.
//
// Picked values:
//
//	docVal  = 2^53 + 1  (not exactly representable as float64; rounds to 2^53)
//	threshold = 2^53    (exactly representable as float64)
//
// Sanity: float64(docVal) == float64(threshold).  So any compare that goes
// through float64 returns "equal" instead of "greater". The integer compare
// must be used.
func TestMatchesFilter_LargeInt64_Precise(t *testing.T) {
	const docVal = int64(1<<53) + 1
	const threshold = int64(1 << 53)

	// Sanity: confirm float64 collapses the two values so the assertions below
	// are exercising the precision fix.
	require.Equal(t, float64(docVal), float64(threshold),
		"sanity: %d and %d round to the same float64", docVal, threshold)
	require.False(t, math.IsNaN(float64(docVal)))

	doc := &firestorev1.Document{Fields: map[string]*firestorev1.Value{
		"count": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: docVal}},
	}}

	// where('count', '>', 2^53) must match a doc with count=2^53+1.
	filterGreater := &store.CompositeFilter{Filters: []store.FieldFilter{{
		Field: "count",
		Op:    store.FilterOpGreaterThan,
		Value: store.FilterValue{Kind: store.FilterValueInt, IntVal: threshold},
	}}}
	require.True(t, codec.MatchesFilter(doc, filterGreater),
		"int64 %d should be > %d (float64 rounding must not erase the difference)",
		docVal, threshold)

	// where('count', '<=', 2^53) must NOT match a doc with count=2^53+1.
	filterLessEq := &store.CompositeFilter{Filters: []store.FieldFilter{{
		Field: "count",
		Op:    store.FilterOpLessThanOrEqual,
		Value: store.FilterValue{Kind: store.FilterValueInt, IntVal: threshold},
	}}}
	require.False(t, codec.MatchesFilter(doc, filterLessEq),
		"int64 %d should NOT be <= %d", docVal, threshold)
}
