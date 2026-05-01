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
	FilterValueArray // used only in IN / array-contains-any operands
	FilterValueJSON  // raw protojson for map/array element comparison; StrVal holds the JSON
)

// FilterValue is a typed scalar or array value for query filter predicates.
type FilterValue struct {
	Kind      FilterValueKind
	BoolVal   bool
	IntVal    int64
	DoubleVal float64
	StrVal    string
	TimeVal   time.Time
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
	Field string
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
	Values []FilterValue
	Before bool
	IsEnd  bool
}

// Query is the full structured query passed to StorageAdapter.QueryDocuments.
type Query struct {
	Parent       string
	CollectionID string
	Filter       *CompositeFilter
	OrderBy      []OrderBy
	Limit        int32
	StartCursor  *Cursor
	EndCursor    *Cursor
	PageToken    string
	PageSize     int32
}

// AggregationOp identifies an aggregation function.
type AggregationOp int8

const (
	AggregationCount AggregationOp = iota
	AggregationSum
	AggregationAvg
)

// Aggregation describes one aggregate column in a RunAggregationQuery.
type Aggregation struct {
	Op    AggregationOp
	Field string // field path; empty for COUNT
	Alias string // key in the response AggregateFields map
}

// AggregationQuery is a base query plus one or more aggregate operations.
type AggregationQuery struct {
	Base         *Query
	Aggregations []Aggregation
}

// AggregateValue is the result of a single aggregation.
// Exactly one of IsNull, IsInt, IsFloat is true.
type AggregateValue struct {
	IsNull  bool
	IsInt   bool
	IntVal  int64
	IsFloat bool
	FloatVal float64
}
