package codec

import (
	"math"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"github.com/petereon/firstyr/internal/store"
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

	if sq.GetStartAt() != nil || sq.GetEndAt() != nil {
		return nil, status.Error(codes.Unimplemented, "query cursors (startAt/endAt) are not yet supported")
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
	v, ok := fields[ff.Field]

	// Field is absent.
	if !ok {
		switch ff.Op {
		case store.FilterOpNotEqual, store.FilterOpNotIn:
			return true
		default:
			return false
		}
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
	case store.FilterValueTime:
		return a.TimeVal.Equal(b.TimeVal)
	}
	return false
}

// filterValuesCmp returns -1, 0, or +1 comparing a to b.
// Only meaningful for scalar types; returns 0 for unknowns.
func filterValuesCmp(a, b store.FilterValue) int {
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
