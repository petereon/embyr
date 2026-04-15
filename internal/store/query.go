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
	FilterValueArray  // used only in IN / array-contains-any operands
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
