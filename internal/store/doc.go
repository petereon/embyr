package store

import "time"

// WriteMode controls the existence precondition for UpdateDocument.
type WriteMode int8

const (
	// WriteModeUpsert creates the document if absent, updates if present.
	WriteModeUpsert WriteMode = iota
	// WriteModeUpdate requires the document to already exist.
	WriteModeUpdate
	// WriteModeInsertOnly requires the document to be absent (pure create).
	WriteModeInsertOnly
)

// Document is the backend-neutral representation of a Firestore document.
// Data is a protojson-encoded blob: {"fields":{"k":{"stringValue":"v"}}}.
// This format round-trips all Firestore Value types losslessly.
type Document struct {
	Path      string
	Data      string    // protojson {"fields":{...}}, or "{}" for empty docs
	CreatedAt time.Time
	UpdatedAt time.Time
	Version   int64
}

// ListPage is the result of a ListDocuments call.
type ListPage struct {
	Documents     []*Document
	NextPageToken string // empty string = last page
}

// WriteOpType identifies the kind of write in a WriteOp.
type WriteOpType int8

const (
	WriteOpUpdate WriteOpType = iota // create or update
	WriteOpDelete                    // delete the document
)

// WriteOp is a single write operation for batch/transaction commits.
type WriteOp struct {
	Type WriteOpType
	Doc  *Document // set for WriteOpUpdate; Doc.Path is the document path
	Path string    // set for WriteOpDelete; the full Firestore document path
	Mode WriteMode // ignored for WriteOpDelete
}

// WriteResult is the per-write result returned by CommitTransaction and BatchWrite.
type WriteResult struct {
	UpdatedAt time.Time
}

// CommitResult holds the results of a CommitTransaction call.
type CommitResult struct {
	WriteResults []WriteResult
	CommitTime   time.Time
}
