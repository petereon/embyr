package codec

import (
	"fmt"
	"math"

	firestorev1 "github.com/petereon/embyr/gen/go/google/firestore/v1"
	"github.com/petereon/embyr/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
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
		// Map and other complex types: serialize to protojson so the SQL layer
		// can compare the exact JSON blob against stored array elements.
		if b, err := protojson.Marshal(v); err == nil {
			return store.FilterValue{Kind: store.FilterValueJSON, StrVal: string(b)}
		}
		return store.FilterValue{Kind: store.FilterValueString, StrVal: v.String()}
	}
}

// QueryFromStructuredQuery builds a store.Query from a Firestore StructuredQuery proto.
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

	if c := sq.GetStartAt(); c != nil {
		q.StartCursor = protoCursor(c, false)
	}
	if c := sq.GetEndAt(); c != nil {
		q.EndCursor = protoCursor(c, true)
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
	if uf := f.GetUnaryFilter(); uf != nil {
		field := uf.GetField().GetFieldPath()
		switch uf.GetOp() {
		case firestorev1.StructuredQuery_UnaryFilter_IS_NULL:
			return store.FieldFilter{Field: field, Op: store.FilterOpEqual, Value: store.FilterValue{Kind: store.FilterValueNull}}, nil
		case firestorev1.StructuredQuery_UnaryFilter_IS_NOT_NULL:
			return store.FieldFilter{Field: field, Op: store.FilterOpNotEqual, Value: store.FilterValue{Kind: store.FilterValueNull}}, nil
		default:
			return store.FieldFilter{}, status.Errorf(codes.Unimplemented, "unary filter op %v not supported", uf.GetOp())
		}
	}
	ff := f.GetFieldFilter()
	if ff == nil {
		return store.FieldFilter{}, status.Error(codes.InvalidArgument, "expected a field or unary filter")
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
		firestorev1.StructuredQuery_FieldFilter_EQUAL:                 store.FilterOpEqual,
		firestorev1.StructuredQuery_FieldFilter_NOT_EQUAL:             store.FilterOpNotEqual,
		firestorev1.StructuredQuery_FieldFilter_LESS_THAN:             store.FilterOpLessThan,
		firestorev1.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL:    store.FilterOpLessThanOrEqual,
		firestorev1.StructuredQuery_FieldFilter_GREATER_THAN:          store.FilterOpGreaterThan,
		firestorev1.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL: store.FilterOpGreaterThanOrEqual,
		firestorev1.StructuredQuery_FieldFilter_IN:                    store.FilterOpIn,
		firestorev1.StructuredQuery_FieldFilter_NOT_IN:                store.FilterOpNotIn,
		firestorev1.StructuredQuery_FieldFilter_ARRAY_CONTAINS:        store.FilterOpArrayContains,
		firestorev1.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY:    store.FilterOpArrayContainsAny,
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

// AggregationQueryFromProto converts a StructuredAggregationQuery proto and its
// parent path into a store.AggregationQuery.
func AggregationQueryFromProto(parent string, saq *firestorev1.StructuredAggregationQuery) (*store.AggregationQuery, error) {
	if saq == nil {
		return nil, status.Error(codes.InvalidArgument, "structured_aggregation_query is required")
	}
	sq := saq.GetStructuredQuery()
	if sq == nil {
		return nil, status.Error(codes.InvalidArgument, "structured_aggregation_query.structured_query is required")
	}

	base, err := QueryFromStructuredQuery(parent, sq, 0, "")
	if err != nil {
		return nil, err
	}

	aggs := saq.GetAggregations()
	if len(aggs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one aggregation is required")
	}

	storeAggs := make([]store.Aggregation, len(aggs))
	for i, a := range aggs {
		alias := a.GetAlias()
		if alias == "" {
			alias = fmt.Sprintf("field_%d", i)
		}
		switch op := a.GetOperator().(type) {
		case *firestorev1.StructuredAggregationQuery_Aggregation_Count_:
			_ = op
			storeAggs[i] = store.Aggregation{Op: store.AggregationCount, Alias: alias}
		case *firestorev1.StructuredAggregationQuery_Aggregation_Sum_:
			storeAggs[i] = store.Aggregation{Op: store.AggregationSum, Field: op.Sum.GetField().GetFieldPath(), Alias: alias}
		case *firestorev1.StructuredAggregationQuery_Aggregation_Avg_:
			storeAggs[i] = store.Aggregation{Op: store.AggregationAvg, Field: op.Avg.GetField().GetFieldPath(), Alias: alias}
		default:
			return nil, status.Errorf(codes.Unimplemented, "aggregation operator not supported")
		}
	}

	return &store.AggregationQuery{Base: base, Aggregations: storeAggs}, nil
}

func protoCursor(c *firestorev1.Cursor, isEnd bool) *store.Cursor {
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

// lookupNestedField traverses dot-notation field paths through Firestore
// MapValue chains, mirroring how the SQL layer descends "fields → mapValue.fields".
// Returns (value, true) on success, (_, false) when any segment is missing or a
// non-map intermediate is encountered.
func lookupNestedField(fields map[string]*firestorev1.Value, fp string) (*firestorev1.Value, bool) {
	if fp == "" {
		return nil, false
	}
	// Split lazily on '.' so we don't allocate a slice for the common case.
	for {
		idx := -1
		for i := 0; i < len(fp); i++ {
			if fp[i] == '.' {
				idx = i
				break
			}
		}
		if idx < 0 {
			v, ok := fields[fp]
			if !ok || v == nil {
				return nil, false
			}
			return v, true
		}
		head := fp[:idx]
		fp = fp[idx+1:]
		v, ok := fields[head]
		if !ok || v == nil {
			return nil, false
		}
		mv := v.GetMapValue()
		if mv == nil {
			return nil, false
		}
		fields = mv.GetFields()
	}
}

// MatchesFilter reports whether doc satisfies the composite filter.
// Returns true when filter is nil (no filter ⇒ all documents match).
// Used by the Listen stream to decide which live-change notifications to
// forward to a given target without a round-trip back to the storage layer.
func MatchesFilter(doc *firestorev1.Document, f *store.CompositeFilter) bool {
	if f == nil {
		return true
	}
	for _, ff := range f.Filters {
		if !matchFieldFilter(doc.GetFields(), ff) {
			return false
		}
	}
	return true
}

func matchFieldFilter(fields map[string]*firestorev1.Value, ff store.FieldFilter) bool {
	v, ok := lookupNestedField(fields, ff.Field)

	// Firestore semantics: a document missing the filtered field is excluded
	// from EVERY predicate, including != and not-in. Mirrors what the SQL
	// layer does (NULL comparisons return NULL → row excluded).
	if !ok {
		return false
	}

	docVal := FilterValueFromProto(v)
	switch ff.Op {
	case store.FilterOpEqual:
		return filterValuesEqual(docVal, ff.Value)
	case store.FilterOpNotEqual:
		return !filterValuesEqual(docVal, ff.Value)
	case store.FilterOpLessThan:
		return filterValuesCmp(docVal, ff.Value) < 0
	case store.FilterOpLessThanOrEqual:
		return filterValuesCmp(docVal, ff.Value) <= 0
	case store.FilterOpGreaterThan:
		return filterValuesCmp(docVal, ff.Value) > 0
	case store.FilterOpGreaterThanOrEqual:
		return filterValuesCmp(docVal, ff.Value) >= 0
	case store.FilterOpIn:
		for _, candidate := range ff.Value.ArrayVals {
			if filterValuesEqual(docVal, candidate) {
				return true
			}
		}
		return false
	case store.FilterOpNotIn:
		for _, candidate := range ff.Value.ArrayVals {
			if filterValuesEqual(docVal, candidate) {
				return false
			}
		}
		return true
	case store.FilterOpArrayContains:
		if docVal.Kind != store.FilterValueArray {
			return false
		}
		for _, el := range docVal.ArrayVals {
			if filterValuesEqual(el, ff.Value) {
				return true
			}
		}
		return false
	case store.FilterOpArrayContainsAny:
		if docVal.Kind != store.FilterValueArray {
			return false
		}
		for _, el := range docVal.ArrayVals {
			for _, candidate := range ff.Value.ArrayVals {
				if filterValuesEqual(el, candidate) {
					return true
				}
			}
		}
		return false
	}
	return false
}

// filterValuesEqual compares two store.FilterValues for equality.
func filterValuesEqual(a, b store.FilterValue) bool {
	if a.Kind != b.Kind {
		// Cross-type int/double comparison.
		if a.Kind == store.FilterValueInt && b.Kind == store.FilterValueDouble {
			return float64(a.IntVal) == b.DoubleVal
		}
		if a.Kind == store.FilterValueDouble && b.Kind == store.FilterValueInt {
			return a.DoubleVal == float64(b.IntVal)
		}
		return false
	}
	switch a.Kind {
	case store.FilterValueNull:
		return true
	case store.FilterValueBool:
		return a.BoolVal == b.BoolVal
	case store.FilterValueInt:
		return a.IntVal == b.IntVal
	case store.FilterValueDouble:
		return a.DoubleVal == b.DoubleVal
	case store.FilterValueString:
		return a.StrVal == b.StrVal
	case store.FilterValueJSON:
		// Both sides are protojson blobs; compare as strings (codec always
		// produces deterministic output for the same proto value).
		return a.StrVal == b.StrVal
	case store.FilterValueTime:
		return a.TimeVal.Equal(b.TimeVal)
	}
	return false
}

// filterValuesCmp returns -1, 0, or +1 comparing a to b.
// Only meaningful for scalar types; returns 0 for unknowns.
func filterValuesCmp(a, b store.FilterValue) int {
	// int64 ↔ int64: compare as int64 to preserve precision for values > 2^53.
	if a.Kind == store.FilterValueInt && b.Kind == store.FilterValueInt {
		switch {
		case a.IntVal < b.IntVal:
			return -1
		case a.IntVal > b.IntVal:
			return 1
		default:
			return 0
		}
	}
	// Otherwise fall back to float64 for cross-type / double compares.
	toFloat := func(v store.FilterValue) (float64, bool) {
		switch v.Kind {
		case store.FilterValueInt:
			return float64(v.IntVal), true
		case store.FilterValueDouble:
			return v.DoubleVal, true
		}
		return 0, false
	}
	if fa, ok := toFloat(a); ok {
		if fb, ok := toFloat(b); ok {
			switch {
			case math.IsNaN(fa) || math.IsNaN(fb):
				return 0
			case fa < fb:
				return -1
			case fa > fb:
				return 1
			default:
				return 0
			}
		}
	}
	if a.Kind == store.FilterValueString && b.Kind == store.FilterValueString {
		if a.StrVal < b.StrVal {
			return -1
		}
		if a.StrVal > b.StrVal {
			return 1
		}
		return 0
	}
	if a.Kind == store.FilterValueTime && b.Kind == store.FilterValueTime {
		if a.TimeVal.Before(b.TimeVal) {
			return -1
		}
		if a.TimeVal.After(b.TimeVal) {
			return 1
		}
		return 0
	}
	return 0
}
