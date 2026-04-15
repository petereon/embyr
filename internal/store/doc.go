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
