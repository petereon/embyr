package store

// DocChangeKind identifies whether a document was created/updated or deleted.
type DocChangeKind int8

const (
	DocChangeUpsert DocChangeKind = iota // document was inserted or updated
	DocChangeDelete                      // document was deleted
)

// DocChange is a single document write event emitted by a storage adapter
// after a committed write. It carries the minimum information needed by the
// Listen handler to route and deliver the change to matching subscribers.
type DocChange struct {
	Path       string        // full Firestore document path
	Collection string        // immediate collection name (derived from path)
	Parent     string        // parent path (derived from path)
	Kind       DocChangeKind
	Version    int64 // new version after the write (0 for deletes)
	// Data contains the full JSON data of the new document state.
	// Empty string for DocChangeDelete.
	Data string
}
