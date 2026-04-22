# firstyr Plan 4 — Real-time Listeners (onSnapshot + BrowserChannel)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Firestore `Listen` RPC for real-time `onSnapshot` support, including the BrowserChannel HTTP transport required by the Firebase JS SDK, PostgreSQL `LISTEN/NOTIFY`, and SQLite WAL hook change delivery.

**Architecture:** Change notifications flow outward from storage through a central fan-out registry to gRPC Listen streams. Storage adapters each implement `Subscribe() (<-chan DocChange, func())` — a broadcast channel to which they push after every committed write. The server-layer `Listen` handler subscribes, filters by target (collection path + optional query), and streams `ListenResponse` protos. BrowserChannel (`/google.firestore.v1.Firestore/Listen/channel`) is a thin HTTP ↔ gRPC bridge in `internal/webchannel/` that maps the Firebase SDK's proprietary HTTP transport to the same gRPC `Listen` handler.

**Tech Stack:** Go, `pgx/v5` `pgconn.WaitForNotification` for NOTIFY, `modernc.org/sqlite` WAL update hook via CGo-free `sqlite.RegisterUpdateHook`, BrowserChannel protocol v8, gRPC-Web binary framing (5-byte header) for message encoding.

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `internal/store/change.go` | **Create** | `DocChange` type |
| `internal/store/adapter.go` | **Modify** | Add `Subscribe() (<-chan DocChange, func())` |
| `internal/store/sqlite/sqlite.go` | **Modify** | WAL update hook; implement `Subscribe` |
| `internal/store/postgres/postgres.go` | **Modify** | `pg_notify` trigger + `LISTEN`; implement `Subscribe` |
| `migrations/postgres/000002_notify_trigger.up.sql` | **Create** | `pg_notify` trigger on documents |
| `migrations/postgres/000002_notify_trigger.down.sql` | **Create** | Drop trigger |
| `internal/listen/registry.go` | **Create** | Fan-out registry: DocChange → []subscriber goroutines |
| `internal/server/listen.go` | **Create** | `Listen` gRPC bidirectional streaming handler |
| `internal/webchannel/session.go` | **Create** | BrowserChannel session state manager |
| `internal/webchannel/handler.go` | **Create** | HTTP POST/GET `/Listen/channel` handlers |
| `internal/server/server.go` | **Modify** | Wire registry, BrowserChannel endpoints, update `readyz` |
| `demo-react/src/firebase.js` | **Modify** | Switch to `firebase/firestore` (full SDK) |
| `demo-react/src/App.jsx` | **Modify** | Add `onSnapshot` live updates |

---

## Task 1: DocChange type + StorageAdapter Subscribe

**Files:**
- Create: `internal/store/change.go`
- Modify: `internal/store/adapter.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/store/change_test.go
package store_test

import (
    "testing"
    "github.com/petereon/firstyr/internal/store"
    "github.com/stretchr/testify/require"
)

func TestDocChange_Fields(t *testing.T) {
    c := store.DocChange{
        Path:       "projects/p/databases/d/documents/col/doc1",
        Collection: "col",
        Parent:     "projects/p/databases/d/documents",
        Kind:       store.DocChangeUpsert,
        Version:    3,
    }
    require.Equal(t, "col", c.Collection)
    require.Equal(t, store.DocChangeUpsert, c.Kind)
}
```

- [ ] **Step 2: Run test, verify it fails**

```
go test ./internal/store/... -run TestDocChange -v
```
Expected: FAIL — `store.DocChange` undefined.

- [ ] **Step 3: Create `internal/store/change.go`**

```go
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
    Version    int64         // new version after the write (0 for deletes)
    // Data contains the full JSON data of the new document state.
    // Empty string for DocChangeDelete.
    Data string
}
```

- [ ] **Step 4: Add `Subscribe` to `internal/store/adapter.go`**

Append to the `StorageAdapter` interface:

```go
    // Subscribe returns a channel that receives DocChange events for every
    // committed write to this database. The caller must invoke the returned
    // cancel function when it no longer needs the subscription to free resources.
    // Multiple concurrent subscribers are supported; each receives all changes.
    Subscribe() (<-chan DocChange, func())
```

- [ ] **Step 5: Run build (adapters will fail to compile — expected)**

```
go build ./... 2>&1
```
Expected: compile errors in sqlite and postgres — "does not implement StorageAdapter".

- [ ] **Step 6: Commit**

```bash
git add internal/store/change.go internal/store/change_test.go internal/store/adapter.go
git commit -m "feat(store): DocChange type and Subscribe interface for real-time listeners"
```

---

## Task 2: Listener registry (fan-out)

**Files:**
- Create: `internal/listen/registry.go`

The registry receives `DocChange` events from storage adapters and fans out to all active subscribers. This is the single source of truth for routing changes to Listen streams.

- [ ] **Step 1: Write the failing test**

```go
// internal/listen/registry_test.go
package listen_test

import (
    "testing"
    "time"

    "github.com/petereon/firstyr/internal/listen"
    "github.com/petereon/firstyr/internal/store"
    "github.com/stretchr/testify/require"
)

func TestRegistry_DispatchAndReceive(t *testing.T) {
    r := listen.NewRegistry()

    ch, unsub := r.Subscribe()
    defer unsub()

    change := store.DocChange{
        Path:       "projects/p/databases/d/documents/col/doc1",
        Collection: "col",
        Parent:     "projects/p/databases/d/documents",
        Kind:       store.DocChangeUpsert,
        Version:    1,
        Data:       `{"fields":{}}`,
    }
    r.Dispatch(change)

    select {
    case got := <-ch:
        require.Equal(t, change.Path, got.Path)
    case <-time.After(time.Second):
        t.Fatal("timeout waiting for change")
    }
}

func TestRegistry_MultipleSubscribers(t *testing.T) {
    r := listen.NewRegistry()

    ch1, unsub1 := r.Subscribe()
    ch2, unsub2 := r.Subscribe()
    defer unsub1()
    defer unsub2()

    r.Dispatch(store.DocChange{Path: "p/d", Kind: store.DocChangeUpsert})

    timeout := time.After(time.Second)
    for i := 0; i < 2; i++ {
        select {
        case <-ch1: // ok
        case <-ch2: // ok
        case <-timeout:
            t.Fatalf("timeout after %d of 2 receives", i)
        }
    }
}

func TestRegistry_Unsubscribe(t *testing.T) {
    r := listen.NewRegistry()
    _, unsub := r.Subscribe()
    unsub()
    // Should not block or panic when dispatching after unsub.
    r.Dispatch(store.DocChange{Path: "p"})
}
```

- [ ] **Step 2: Run test, verify it fails**

```
go test ./internal/listen/... -v
```
Expected: FAIL — package `listen` not found.

- [ ] **Step 3: Create `internal/listen/registry.go`**

```go
package listen

import (
    "sync"

    "github.com/petereon/firstyr/internal/store"
)

// Registry fan-outs DocChange events to all active subscribers.
// Each subscriber gets a buffered channel; slow subscribers drop changes
// rather than blocking the dispatch goroutine.
type Registry struct {
    mu   sync.RWMutex
    subs map[uint64]chan store.DocChange
    next uint64
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
    return &Registry{subs: make(map[uint64]chan store.DocChange)}
}

// Subscribe returns a channel that receives all future DocChange events
// and a cancel function. Call cancel when the subscriber no longer needs events.
// The channel is buffered (capacity 64) to avoid blocking Dispatch.
func (r *Registry) Subscribe() (<-chan store.DocChange, func()) {
    ch := make(chan store.DocChange, 64)
    r.mu.Lock()
    id := r.next
    r.next++
    r.subs[id] = ch
    r.mu.Unlock()

    cancel := func() {
        r.mu.Lock()
        delete(r.subs, id)
        r.mu.Unlock()
        // Drain and close to unblock any waiting receiver.
        close(ch)
        for range ch {}
    }
    return ch, cancel
}

// Dispatch sends c to all current subscribers. Non-blocking: if a subscriber's
// buffer is full the change is silently dropped for that subscriber.
func (r *Registry) Dispatch(c store.DocChange) {
    r.mu.RLock()
    defer r.mu.RUnlock()
    for _, ch := range r.subs {
        select {
        case ch <- c:
        default: // subscriber is slow; drop rather than block
        }
    }
}
```

- [ ] **Step 4: Run tests, verify they pass**

```
go test ./internal/listen/... -v
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/listen/registry.go internal/listen/registry_test.go
git commit -m "feat(listen): fan-out registry for DocChange dispatch"
```

---

## Task 3: PostgreSQL LISTEN/NOTIFY migration + adapter implementation

**Files:**
- Create: `migrations/postgres/000002_notify_trigger.up.sql`
- Create: `migrations/postgres/000002_notify_trigger.down.sql`
- Modify: `internal/store/postgres/postgres.go`
- Test: `internal/store/postgres/postgres_test.go`

PostgreSQL uses a SQL trigger to call `pg_notify` after every write, plus a single persistent `LISTEN` connection on a `pgx` raw conn that feeds the registry.

- [ ] **Step 1: Create the migration files**

`migrations/postgres/000002_notify_trigger.up.sql`:
```sql
CREATE OR REPLACE FUNCTION notify_doc_change() RETURNS TRIGGER AS $$
DECLARE
    payload TEXT;
    kind TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        kind := 'delete';
        payload := json_build_object(
            'path',       OLD.path,
            'collection', OLD.collection,
            'parent',     OLD.parent,
            'kind',       kind,
            'version',    OLD.version,
            'data',       ''
        )::text;
    ELSE
        kind := 'upsert';
        payload := json_build_object(
            'path',       NEW.path,
            'collection', NEW.collection,
            'parent',     NEW.parent,
            'kind',       kind,
            'version',    NEW.version,
            'data',       NEW.data::text
        )::text;
    END IF;
    -- pg_notify payload is limited to 8000 bytes.
    -- Truncate data to stay under the limit; the Listen handler
    -- re-fetches the full document when data is empty.
    IF length(payload) > 7500 THEN
        IF TG_OP = 'DELETE' THEN
            payload := json_build_object(
                'path', OLD.path, 'collection', OLD.collection,
                'parent', OLD.parent, 'kind', kind, 'version', OLD.version, 'data', ''
            )::text;
        ELSE
            payload := json_build_object(
                'path', NEW.path, 'collection', NEW.collection,
                'parent', NEW.parent, 'kind', kind, 'version', NEW.version, 'data', ''
            )::text;
        END IF;
    END IF;
    PERFORM pg_notify('doc_changes', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_doc_changes
AFTER INSERT OR UPDATE OR DELETE ON documents
FOR EACH ROW EXECUTE FUNCTION notify_doc_change();
```

`migrations/postgres/000002_notify_trigger.down.sql`:
```sql
DROP TRIGGER IF EXISTS trg_doc_changes ON documents;
DROP FUNCTION IF EXISTS notify_doc_change();
```

- [ ] **Step 2: Write the failing test**

Add to `internal/store/postgres/postgres_test.go`:

```go
func TestSubscribe_ReceivesChange(t *testing.T) {
    // Requires a live PostgreSQL instance. Skip if DSN not set.
    a := newTestAdapter(t)
    ch, cancel := a.Subscribe()
    defer cancel()

    ctx := context.Background()
    _, err := a.CreateDocument(ctx, &store.Document{
        Path: "projects/p/databases/d/documents/sub/test1",
        Data: `{"fields":{"x":{"integerValue":"1"}}}`,
    })
    require.NoError(t, err)

    select {
    case change := <-ch:
        require.Equal(t, "projects/p/databases/d/documents/sub/test1", change.Path)
        require.Equal(t, store.DocChangeUpsert, change.Kind)
    case <-time.After(3 * time.Second):
        t.Fatal("timeout waiting for change notification")
    }
}
```

- [ ] **Step 3: Run test, verify it fails**

```
go test ./internal/store/postgres/... -run TestSubscribe -v
```
Expected: FAIL — `Subscribe` method not implemented.

- [ ] **Step 4: Add `Subscribe` to postgres adapter**

The adapter needs a `pgx` raw connection for NOTIFY (`database/sql` does not expose `LISTEN`). The adapter must hold a `*pgx.Conn` alongside the `*sql.DB`:

```go
import (
    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/pgconn"
    "encoding/json"
)

type Adapter struct {
    rawDB          *sql.DB
    db             sqlExecer
    migrationsPath string
    dsn            string    // stored for pgx raw conn in Subscribe
}

// Update New to store the DSN:
func New(dsn, migrationsPath string, maxConns int) (*Adapter, error) {
    db, err := sql.Open("pgx", dsn)
    // ...
    return &Adapter{rawDB: db, db: db, migrationsPath: migrationsPath, dsn: dsn}, nil
}
```

Add `Subscribe`:

```go
func (a *Adapter) Subscribe() (<-chan store.DocChange, func()) {
    ch := make(chan store.DocChange, 64)
    ctx, cancel := context.WithCancel(context.Background())

    go func() {
        defer close(ch)
        conn, err := pgx.Connect(ctx, a.dsn)
        if err != nil {
            return
        }
        defer conn.Close(ctx)

        if _, err := conn.Exec(ctx, "LISTEN doc_changes"); err != nil {
            return
        }

        for {
            notif, err := conn.WaitForNotification(ctx)
            if err != nil {
                return // context cancelled or connection lost
            }
            var payload struct {
                Path       string `json:"path"`
                Collection string `json:"collection"`
                Parent     string `json:"parent"`
                Kind       string `json:"kind"`
                Version    int64  `json:"version"`
                Data       string `json:"data"`
            }
            if err := json.Unmarshal([]byte(notif.Payload), &payload); err != nil {
                continue
            }
            kind := store.DocChangeUpsert
            if payload.Kind == "delete" {
                kind = store.DocChangeDelete
            }
            c := store.DocChange{
                Path:       payload.Path,
                Collection: payload.Collection,
                Parent:     payload.Parent,
                Kind:       kind,
                Version:    payload.Version,
                Data:       payload.Data,
            }
            select {
            case ch <- c:
            case <-ctx.Done():
                return
            }
        }
    }()

    return ch, cancel
}
```

Note: `conn.WaitForNotification` is from `pgx/v5`. Verify the import path in go.mod: `github.com/jackc/pgx/v5`.

- [ ] **Step 5: Run tests, verify they pass**

```
go test ./internal/store/postgres/... -run TestSubscribe -v
```
Expected: PASS (requires live PostgreSQL; skipped if not available).

- [ ] **Step 6: Commit**

```bash
git add migrations/postgres/000002_notify_trigger.up.sql \
        migrations/postgres/000002_notify_trigger.down.sql \
        internal/store/postgres/postgres.go \
        internal/store/postgres/postgres_test.go
git commit -m "feat(postgres): pg_notify trigger + Subscribe for real-time change delivery"
```

---

## Task 4: SQLite WAL hook + Subscribe

**Files:**
- Modify: `internal/store/sqlite/sqlite.go`
- Test: `internal/store/sqlite/sqlite_test.go`

SQLite's update hook fires synchronously after each committed write in-process. `modernc.org/sqlite` exposes this via `(*sqlite.Conn).RegisterUpdateHook`.

- [ ] **Step 1: Write the failing test**

Add to `internal/store/sqlite/sqlite_test.go`:

```go
func TestSubscribe_ReceivesChange(t *testing.T) {
    a := newTestAdapter(t)
    ch, cancel := a.Subscribe()
    defer cancel()

    _, err := a.CreateDocument(context.Background(), &store.Document{
        Path: "projects/p/databases/d/documents/sub/doc1",
        Data: `{"fields":{"y":{"stringValue":"hello"}}}`,
    })
    require.NoError(t, err)

    select {
    case change := <-ch:
        require.Equal(t, "projects/p/databases/d/documents/sub/doc1", change.Path)
        require.Equal(t, store.DocChangeUpsert, change.Kind)
    case <-time.After(time.Second):
        t.Fatal("timeout waiting for change")
    }
}
```

- [ ] **Step 2: Run test, verify it fails**

```
go test ./internal/store/sqlite/... -run TestSubscribe -v
```
Expected: FAIL.

- [ ] **Step 3: Add subscriber registry + WAL hook to SQLite adapter**

`modernc.org/sqlite` exposes the update hook via its raw `*sqlite.Conn` (not `*sql.DB`). To access it, open the database using the `modernc.org/sqlite` driver directly alongside the `database/sql` pool:

```go
import (
    "sync"
    modsqlite "modernc.org/sqlite"
)

type Adapter struct {
    rawDB          *sql.DB
    db             sqlExecer
    migrationsPath string
    // WAL hook subscriber list
    subsMu sync.RWMutex
    subs   map[uint64]chan store.DocChange
    subNext uint64
}
```

The WAL update hook approach using `database/sql` doesn't expose the raw driver conn directly, but `modernc.org/sqlite` allows registering hooks via the `*sql.DB` using a connection hook. The recommended pattern is to open a **second** connection (`sql.Open`) for the WAL hook only, since `database/sql` pools connections and the hook registers on a specific `*sqlite.Conn`.

A simpler approach that avoids this: **intercept writes at the adapter layer** instead of using the WAL hook. After every successful `CreateDocument`, `UpdateDocument`, `DeleteDocument` call, broadcast the change to subscribers:

```go
// notifySubscribers sends c to all current subscribers (non-blocking).
func (a *Adapter) notifySubscribers(c store.DocChange) {
    a.subsMu.RLock()
    defer a.subsMu.RUnlock()
    for _, ch := range a.subs {
        select {
        case ch <- c:
        default:
        }
    }
}

func (a *Adapter) Subscribe() (<-chan store.DocChange, func()) {
    ch := make(chan store.DocChange, 64)
    a.subsMu.Lock()
    id := a.subNext
    a.subNext++
    if a.subs == nil { a.subs = make(map[uint64]chan store.DocChange) }
    a.subs[id] = ch
    a.subsMu.Unlock()
    return ch, func() {
        a.subsMu.Lock()
        delete(a.subs, id)
        a.subsMu.Unlock()
        close(ch)
        for range ch {}
    }
}
```

Update `CreateDocument`, `UpdateDocument`, `DeleteDocument` to call `notifySubscribers` on success:

In `CreateDocument`, after `return d, nil`:
```go
    a.notifySubscribers(store.DocChange{
        Path: d.Path, Collection: collection, Parent: parent,
        Kind: store.DocChangeUpsert, Version: d.Version, Data: d.Data,
    })
    return d, nil
```

In `UpdateDocument` (all three modes), similarly after the successful return.

In `DeleteDocument`, after the successful delete:
```go
    collection, parent := sqliteParseCollection(path)
    a.notifySubscribers(store.DocChange{
        Path: path, Collection: collection, Parent: parent,
        Kind: store.DocChangeDelete,
    })
    return nil
```

This approach is simpler than the WAL hook and equally correct for single-process use. The WAL hook would be needed only for detecting writes from other processes sharing the same SQLite file — not the firstyr use case.

- [ ] **Step 4: Run tests, verify they pass**

```
go test ./internal/store/sqlite/... -run TestSubscribe -v
go test ./internal/store/sqlite/... 2>&1
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store/sqlite/sqlite.go internal/store/sqlite/sqlite_test.go
git commit -m "feat(sqlite): Subscribe with post-write notification for real-time changes"
```

---

## Task 5: Resume tokens

Resume tokens let the SDK reconnect and receive changes it missed while offline. We encode `UpdatedAt` as the token; on reconnect, replay any document updated after that timestamp.

**Files:**
- Create: `internal/listen/token.go`

- [ ] **Step 1: Write failing test**

```go
// internal/listen/token_test.go
package listen_test

import (
    "testing"
    "time"
    "github.com/petereon/firstyr/internal/listen"
    "github.com/stretchr/testify/require"
)

func TestResumeToken_RoundTrip(t *testing.T) {
    ts := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
    tok := listen.EncodeResumeToken(ts)
    require.NotEmpty(t, tok)
    got, err := listen.DecodeResumeToken(tok)
    require.NoError(t, err)
    require.True(t, ts.Equal(got))
}

func TestResumeToken_InvalidInput(t *testing.T) {
    _, err := listen.DecodeResumeToken([]byte("not-base64!!!"))
    require.Error(t, err)
}
```

- [ ] **Step 2: Run test, verify it fails**

```
go test ./internal/listen/... -run TestResumeToken -v
```
Expected: FAIL.

- [ ] **Step 3: Create `internal/listen/token.go`**

```go
package listen

import (
    "encoding/base64"
    "fmt"
    "time"
)

// EncodeResumeToken encodes a timestamp as an opaque resume token (base64 RFC3339Nano).
func EncodeResumeToken(ts time.Time) []byte {
    s := ts.UTC().Format(time.RFC3339Nano)
    return []byte(base64.RawURLEncoding.EncodeToString([]byte(s)))
}

// DecodeResumeToken decodes a resume token back to a time.Time.
// Returns an error if the token is malformed.
func DecodeResumeToken(tok []byte) (time.Time, error) {
    b, err := base64.RawURLEncoding.DecodeString(string(tok))
    if err != nil {
        return time.Time{}, fmt.Errorf("listen: decode resume token: %w", err)
    }
    ts, err := time.Parse(time.RFC3339Nano, string(b))
    if err != nil {
        return time.Time{}, fmt.Errorf("listen: parse resume token timestamp: %w", err)
    }
    return ts, nil
}
```

- [ ] **Step 4: Run tests, verify they pass**

```
go test ./internal/listen/... -run TestResumeToken -v
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/listen/token.go internal/listen/token_test.go
git commit -m "feat(listen): resume token encode/decode"
```

---

## Task 6: Listen gRPC handler

**Files:**
- Create: `internal/server/listen.go`
- Test: `internal/server/listen_test.go`

The `Listen` RPC is a bidirectional stream. The client sends `ListenRequest` with `AddTarget` / `RemoveTarget`. The server streams `ListenResponse` messages: first the current snapshot (all matching documents), then `CURRENT` state, then live change events.

- [ ] **Step 1: Write failing test**

```go
// internal/server/listen_test.go
package server_test

import (
    "context"
    "io"
    "testing"
    "time"

    firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
    "github.com/stretchr/testify/require"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

func TestListen_InitialSnapshot(t *testing.T) {
    srv, _ := startTestServer(t)
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    gc, err := grpc.NewClient(srv.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    require.NoError(t, err)
    defer gc.Close()

    client := firestorev1.NewFirestoreClient(gc)

    // Seed a document
    _, err = client.CreateDocument(ctx, &firestorev1.CreateDocumentRequest{
        Parent:       "projects/p/databases/(default)/documents",
        CollectionId: "live",
        DocumentId:   "doc1",
        Document: &firestorev1.Document{
            Fields: map[string]*firestorev1.Value{
                "msg": {ValueType: &firestorev1.Value_StringValue{StringValue: "hello"}},
            },
        },
    })
    require.NoError(t, err)

    stream, err := client.Listen(ctx)
    require.NoError(t, err)

    // Send AddTarget for the "live" collection.
    err = stream.Send(&firestorev1.ListenRequest{
        Database: "projects/p/databases/(default)",
        TargetChange: &firestorev1.ListenRequest_AddTarget{
            AddTarget: &firestorev1.Target{
                TargetType: &firestorev1.Target_Query{
                    Query: &firestorev1.Target_QueryTarget{
                        Parent: "projects/p/databases/(default)/documents",
                        QueryType: &firestorev1.Target_QueryTarget_StructuredQuery{
                            StructuredQuery: &firestorev1.StructuredQuery{
                                From: []*firestorev1.StructuredQuery_CollectionSelector{
                                    {CollectionId: "live"},
                                },
                            },
                        },
                    },
                },
                TargetId: 2,
            },
        },
    })
    require.NoError(t, err)

    // Expect: ADD target_change, then document_change(s), then CURRENT target_change.
    gotDocChange := false
    gotCurrent := false
    for !gotCurrent {
        resp, err := stream.Recv()
        if err == io.EOF { break }
        require.NoError(t, err)
        switch r := resp.GetResponseType().(type) {
        case *firestorev1.ListenResponse_DocumentChange:
            require.Equal(t, "projects/p/databases/(default)/documents/live/doc1",
                r.DocumentChange.GetDocument().GetName())
            gotDocChange = true
        case *firestorev1.ListenResponse_TargetChange:
            if r.TargetChange.GetTargetChangeType() == firestorev1.TargetChange_CURRENT {
                gotCurrent = true
            }
        }
    }
    require.True(t, gotDocChange, "expected at least one DocumentChange in snapshot")
    require.True(t, gotCurrent, "expected CURRENT TargetChange after snapshot")
}

func TestListen_LiveChange(t *testing.T) {
    srv, _ := startTestServer(t)
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    gc, err := grpc.NewClient(srv.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    require.NoError(t, err)
    defer gc.Close()
    client := firestorev1.NewFirestoreClient(gc)

    stream, err := client.Listen(ctx)
    require.NoError(t, err)

    require.NoError(t, stream.Send(&firestorev1.ListenRequest{
        Database: "projects/p/databases/(default)",
        TargetChange: &firestorev1.ListenRequest_AddTarget{
            AddTarget: &firestorev1.Target{
                TargetType: &firestorev1.Target_Query{
                    Query: &firestorev1.Target_QueryTarget{
                        Parent: "projects/p/databases/(default)/documents",
                        QueryType: &firestorev1.Target_QueryTarget_StructuredQuery{
                            StructuredQuery: &firestorev1.StructuredQuery{
                                From: []*firestorev1.StructuredQuery_CollectionSelector{{CollectionId: "live2"}},
                            },
                        },
                    },
                },
                TargetId: 2,
            },
        },
    }))

    // Drain the initial snapshot (CURRENT).
    for {
        resp, err := stream.Recv()
        require.NoError(t, err)
        if tc, ok := resp.GetResponseType().(*firestorev1.ListenResponse_TargetChange); ok {
            if tc.TargetChange.GetTargetChangeType() == firestorev1.TargetChange_CURRENT {
                break
            }
        }
    }

    // Create a document AFTER the stream is established.
    _, err = client.CreateDocument(ctx, &firestorev1.CreateDocumentRequest{
        Parent:       "projects/p/databases/(default)/documents",
        CollectionId: "live2",
        DocumentId:   "new1",
        Document:     &firestorev1.Document{},
    })
    require.NoError(t, err)

    // Expect a DocumentChange for the new document.
    for {
        resp, err := stream.Recv()
        require.NoError(t, err)
        if dc, ok := resp.GetResponseType().(*firestorev1.ListenResponse_DocumentChange); ok {
            require.Contains(t, dc.DocumentChange.GetDocument().GetName(), "new1")
            return
        }
    }
}
```

- [ ] **Step 2: Run test, verify it fails**

```
go test ./internal/server/... -run TestListen -v
```
Expected: FAIL — `Listen` returns `Unimplemented`.

- [ ] **Step 3: Create `internal/server/listen.go`**

The `Listen` handler needs access to the registry. Add `registry *listen.Registry` to `firestoreServer`:

In `internal/server/handlers.go`, change the struct:

```go
type firestoreServer struct {
    firestorev1.UnimplementedFirestoreServer
    db       store.StorageAdapter
    log      *zap.Logger
    registry *listen.Registry
}
```

In `internal/server/server.go`, in `New()`:
```go
    reg := listen.NewRegistry()
    fs := &firestoreServer{db: db, log: log, registry: reg}
    // Start feeding the registry from the adapter's Subscribe channel.
    go func() {
        ch, cancel := db.Subscribe()
        defer cancel()
        for c := range ch {
            reg.Dispatch(c)
        }
    }()
```

Now create `internal/server/listen.go`:

```go
package server

import (
    "io"
    "time"

    firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
    "github.com/petereon/firstyr/internal/codec"
    "github.com/petereon/firstyr/internal/listen"
    "github.com/petereon/firstyr/internal/store"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
    "google.golang.org/protobuf/types/known/timestamppb"
)

// Listen implements the Firestore bidirectional streaming Listen RPC.
// It delivers the initial snapshot of each target, then live change events.
func (s *firestoreServer) Listen(stream firestorev1.Firestore_ListenServer) error {
    ctx := stream.Context()

    // Map of target_id → collectionInfo for filtering incoming changes.
    type targetInfo struct {
        parent       string
        collectionID string
        targetID     int32
    }
    targets := make(map[int32]targetInfo)

    // Subscribe to all changes from the storage adapter via the registry.
    changeCh, unsub := s.registry.Subscribe()
    defer unsub()

    // recvCh receives ListenRequests from the client asynchronously.
    recvCh := make(chan *firestorev1.ListenRequest, 8)
    recvErrCh := make(chan error, 1)
    go func() {
        for {
            req, err := stream.Recv()
            if err != nil {
                recvErrCh <- err
                return
            }
            recvCh <- req
        }
    }()

    sendTargetChange := func(changeType firestorev1.TargetChange_TargetChangeType, targetIDs []int32, resumeToken []byte) error {
        tc := &firestorev1.TargetChange{
            TargetChangeType: changeType,
            TargetIds:        targetIDs,
            ReadTime:         timestamppb.Now(),
        }
        if len(resumeToken) > 0 {
            tc.ResumeToken = resumeToken
        }
        return stream.Send(&firestorev1.ListenResponse{
            ResponseType: &firestorev1.ListenResponse_TargetChange{TargetChange: tc},
        })
    }

    deliverSnapshot := func(ti targetInfo) error {
        // Announce the target was added.
        if err := sendTargetChange(firestorev1.TargetChange_ADD, []int32{ti.targetID}, nil); err != nil {
            return err
        }

        // Stream current documents.
        q := &store.Query{
            Parent:       ti.parent,
            CollectionID: ti.collectionID,
            PageSize:     300,
        }
        readTime := time.Now().UTC()
        for {
            page, err := s.db.QueryDocuments(ctx, q)
            if err != nil {
                return err
            }
            for _, sd := range page.Documents {
                proto, err := codec.StoreToProto(sd)
                if err != nil {
                    return status.Errorf(codes.Internal, "decode document: %v", err)
                }
                if err := stream.Send(&firestorev1.ListenResponse{
                    ResponseType: &firestorev1.ListenResponse_DocumentChange{
                        DocumentChange: &firestorev1.DocumentChange{
                            Document:  proto,
                            TargetIds: []int32{ti.targetID},
                        },
                    },
                }); err != nil {
                    return err
                }
            }
            if page.NextPageToken == "" {
                break
            }
            q.PageToken = page.NextPageToken
        }

        // Signal snapshot complete.
        return sendTargetChange(
            firestorev1.TargetChange_CURRENT,
            []int32{ti.targetID},
            listen.EncodeResumeToken(readTime),
        )
    }

    // Keep-alive ticker: send a NO_CHANGE every 30s to prevent proxy timeouts.
    keepAlive := time.NewTicker(30 * time.Second)
    defer keepAlive.Stop()

    for {
        select {
        case <-ctx.Done():
            return ctx.Err()

        case err := <-recvErrCh:
            if err == io.EOF {
                return nil
            }
            return err

        case req := <-recvCh:
            switch tc := req.GetTargetChange().(type) {
            case *firestorev1.ListenRequest_AddTarget:
                t := tc.AddTarget
                qt := t.GetQuery()
                if qt == nil {
                    return status.Error(codes.Unimplemented, "only query targets are supported")
                }
                sq := qt.GetStructuredQuery()
                if sq == nil || len(sq.GetFrom()) == 0 {
                    return status.Error(codes.InvalidArgument, "structured_query.from is required")
                }
                ti := targetInfo{
                    parent:       qt.GetParent(),
                    collectionID: sq.GetFrom()[0].GetCollectionId(),
                    targetID:     t.GetTargetId(),
                }
                targets[ti.targetID] = ti
                if err := deliverSnapshot(ti); err != nil {
                    return err
                }

            case *firestorev1.ListenRequest_RemoveTarget:
                delete(targets, tc.RemoveTarget)
                if err := sendTargetChange(firestorev1.TargetChange_REMOVE, []int32{tc.RemoveTarget}, nil); err != nil {
                    return err
                }
            }

        case change := <-changeCh:
            // Fan out to every target that matches this change's collection + parent.
            for _, ti := range targets {
                if ti.parent != change.Parent || ti.collectionID != change.Collection {
                    continue
                }
                if change.Kind == store.DocChangeDelete {
                    if err := stream.Send(&firestorev1.ListenResponse{
                        ResponseType: &firestorev1.ListenResponse_DocumentDelete{
                            DocumentDelete: &firestorev1.DocumentDelete{
                                Document:  change.Path,
                                TargetIds: []int32{ti.targetID},
                                ReadTime:  timestamppb.Now(),
                            },
                        },
                    }); err != nil {
                        return err
                    }
                } else {
                    // Fetch the full document (change.Data may be truncated for large docs).
                    sd, err := s.db.GetDocument(ctx, change.Path)
                    if err != nil {
                        if status.Code(err) == codes.NotFound {
                            continue // deleted between notify and fetch
                        }
                        return err
                    }
                    proto, err := codec.StoreToProto(sd)
                    if err != nil {
                        return status.Errorf(codes.Internal, "decode document: %v", err)
                    }
                    if err := stream.Send(&firestorev1.ListenResponse{
                        ResponseType: &firestorev1.ListenResponse_DocumentChange{
                            DocumentChange: &firestorev1.DocumentChange{
                                Document:  proto,
                                TargetIds: []int32{ti.targetID},
                            },
                        },
                    }); err != nil {
                        return err
                    }
                }
                // Send a NO_CHANGE resume token after each change batch.
                if err := sendTargetChange(firestorev1.TargetChange_NO_CHANGE, nil,
                    listen.EncodeResumeToken(time.Now().UTC())); err != nil {
                    return err
                }
            }

        case <-keepAlive.C:
            if err := sendTargetChange(firestorev1.TargetChange_NO_CHANGE, nil, nil); err != nil {
                return err
            }
        }
    }
}
```

- [ ] **Step 4: Run tests, verify they pass**

```
go test ./internal/server/... -run TestListen -v -timeout 15s
go test ./... 2>&1
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/server/listen.go internal/server/listen_test.go \
        internal/server/handlers.go internal/server/server.go
git commit -m "feat(server): Listen RPC bidirectional streaming handler with initial snapshot + live changes"
```

---

## Task 7: BrowserChannel session manager

**Files:**
- Create: `internal/webchannel/session.go`

The Firebase JS SDK sends all `Listen` traffic over BrowserChannel — a proprietary HTTP long-polling protocol. Each session has a forward channel (POST) and a back channel (GET). Messages are gRPC-Web framed (5-byte header + binary proto) then base64-encoded.

- [ ] **Step 1: Write failing test**

```go
// internal/webchannel/session_test.go
package webchannel_test

import (
    "testing"
    "github.com/petereon/firstyr/internal/webchannel"
    "github.com/stretchr/testify/require"
)

func TestSession_NewAndEncodeMessage(t *testing.T) {
    mgr := webchannel.NewManager()
    sess := mgr.NewSession()
    require.NotEmpty(t, sess.ID)

    // Encode a trivial proto payload
    data := []byte("hello")
    frame := webchannel.EncodeGRPCWebFrame(data)
    require.Equal(t, byte(0x00), frame[0]) // not compressed
    require.Len(t, frame, 5+len(data))

    msg := sess.FormatDataChunk(frame)
    require.Contains(t, string(msg), sess.ID[:4]) // session ID in connect message
}
```

- [ ] **Step 2: Run test, verify it fails**

```
go test ./internal/webchannel/... -v
```
Expected: FAIL — package `webchannel` not found.

- [ ] **Step 3: Create `internal/webchannel/session.go`**

```go
package webchannel

import (
    "crypto/rand"
    "encoding/base64"
    "encoding/binary"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "sync"
    "sync/atomic"
)

// Manager owns all active BrowserChannel sessions.
type Manager struct {
    mu       sync.RWMutex
    sessions map[string]*Session
}

// NewManager creates an empty Manager.
func NewManager() *Manager {
    return &Manager{sessions: make(map[string]*Session)}
}

// NewSession creates a new Session, registers it, and returns it.
func (m *Manager) NewSession() *Session {
    id := newSessionID()
    s := &Session{
        ID:     id,
        mgr:    m,
        outbox: make(chan []byte, 128),
    }
    m.mu.Lock()
    m.sessions[id] = s
    m.mu.Unlock()
    return s
}

// Get returns the session with the given ID, or nil.
func (m *Manager) Get(id string) *Session {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return m.sessions[id]
}

// Remove deletes the session from the registry.
func (m *Manager) Remove(id string) {
    m.mu.Lock()
    delete(m.sessions, id)
    m.mu.Unlock()
}

func newSessionID() string {
    b := make([]byte, 12)
    _, _ = rand.Read(b)
    return hex.EncodeToString(b)
}

// Session holds the state for a single BrowserChannel connection.
type Session struct {
    ID     string
    mgr    *Manager
    seq    atomic.Int64  // next message sequence number
    outbox chan []byte    // raw BrowserChannel chunks to write to the GET backchannel
}

// Send enqueues a serialized BrowserChannel chunk for delivery to the client.
func (s *Session) Send(chunk []byte) {
    select {
    case s.outbox <- chunk:
    default: // drop if buffer full (slow client)
    }
}

// Outbox returns the output channel for the GET handler to drain.
func (s *Session) Outbox() <-chan []byte {
    return s.outbox
}

// EncodeGRPCWebFrame wraps proto bytes in a gRPC-Web frame:
// [0x00 (no compression)][4-byte big-endian length][proto bytes]
func EncodeGRPCWebFrame(proto []byte) []byte {
    frame := make([]byte, 5+len(proto))
    frame[0] = 0x00
    binary.BigEndian.PutUint32(frame[1:5], uint32(len(proto)))
    copy(frame[5:], proto)
    return frame
}

// DecodeGRPCWebFrame extracts the proto bytes from a gRPC-Web frame.
// Returns the payload bytes, or an error if the frame is malformed.
func DecodeGRPCWebFrame(frame []byte) ([]byte, error) {
    if len(frame) < 5 {
        return nil, fmt.Errorf("webchannel: frame too short (%d bytes)", len(frame))
    }
    length := binary.BigEndian.Uint32(frame[1:5])
    if int(length) > len(frame)-5 {
        return nil, fmt.Errorf("webchannel: frame length %d exceeds data (%d bytes)", length, len(frame)-5)
    }
    return frame[5 : 5+length], nil
}

// FormatConnectChunk returns the BrowserChannel JSON chunk for session establishment.
// Format: <N>\n[[0,["c","<sessionId>","",8,8,0]],[1,["noop"]]]
func (s *Session) FormatConnectChunk() []byte {
    msgs := []interface{}{
        []interface{}{0, []interface{}{"c", s.ID, "", 8, 8, 0}},
        []interface{}{1, []interface{}{"noop"}},
    }
    s.seq.Store(2)
    b, _ := json.Marshal(msgs)
    return []byte(fmt.Sprintf("%d\n%s", len(b), b))
}

// FormatDataChunk wraps a gRPC-Web frame in a BrowserChannel JSON data chunk.
// frame must be a complete gRPC-Web frame (output of EncodeGRPCWebFrame).
func (s *Session) FormatDataChunk(frame []byte) []byte {
    seq := s.seq.Add(1) - 1
    encoded := base64.StdEncoding.EncodeToString(frame)
    msgs := []interface{}{
        []interface{}{seq, []interface{}{encoded}},
    }
    b, _ := json.Marshal(msgs)
    return []byte(fmt.Sprintf("%d\n%s", len(b), b))
}

// FormatNoopChunk returns a keep-alive noop chunk.
func (s *Session) FormatNoopChunk() []byte {
    seq := s.seq.Add(1) - 1
    msgs := []interface{}{
        []interface{}{seq, []interface{}{"noop"}},
    }
    b, _ := json.Marshal(msgs)
    return []byte(fmt.Sprintf("%d\n%s", len(b), b))
}
```

- [ ] **Step 4: Run tests, verify they pass**

```
go test ./internal/webchannel/... -v
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/webchannel/session.go internal/webchannel/session_test.go
git commit -m "feat(webchannel): BrowserChannel session manager + gRPC-Web frame helpers"
```

---

## Task 8: BrowserChannel HTTP handlers + Server wiring

**Files:**
- Create: `internal/webchannel/handler.go`
- Modify: `internal/server/server.go`

The HTTP handler bridges BrowserChannel HTTP ↔ gRPC `Listen` using a fake `Firestore_ListenServer` stream. The forward channel (POST) decodes `req0___data__` base64 → gRPC-Web frame → `ListenRequest` proto and feeds it to the Listen handler. The back channel (GET) drains the session's outbox, sending `ListenResponse` chunks as chunked HTTP.

- [ ] **Step 1: Create `internal/webchannel/handler.go`**

```go
package webchannel

import (
    "context"
    "encoding/base64"
    "io"
    "net/http"
    "strings"
    "sync"
    "time"

    firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
    "google.golang.org/grpc/metadata"
    "google.golang.org/protobuf/proto"
)

// listenBridge is a fake Firestore_ListenServer that routes between HTTP and the gRPC handler.
type listenBridge struct {
    ctx    context.Context
    sendCh chan *firestorev1.ListenResponse
    recvCh chan *firestorev1.ListenRequest
    mu     sync.Mutex
}

func newListenBridge(ctx context.Context) *listenBridge {
    return &listenBridge{
        ctx:    ctx,
        sendCh: make(chan *firestorev1.ListenResponse, 64),
        recvCh: make(chan *firestorev1.ListenRequest, 8),
    }
}

// grpc.ServerStream interface
func (b *listenBridge) SetHeader(metadata.MD) error  { return nil }
func (b *listenBridge) SendHeader(metadata.MD) error { return nil }
func (b *listenBridge) SetTrailer(metadata.MD)        {}
func (b *listenBridge) Context() context.Context      { return b.ctx }
func (b *listenBridge) SendMsg(m any) error           { return nil }
func (b *listenBridge) RecvMsg(m any) error           { return nil }

// Firestore_ListenServer interface
func (b *listenBridge) Send(resp *firestorev1.ListenResponse) error {
    select {
    case b.sendCh <- resp:
        return nil
    case <-b.ctx.Done():
        return b.ctx.Err()
    }
}

func (b *listenBridge) Recv() (*firestorev1.ListenRequest, error) {
    select {
    case req := <-b.recvCh:
        return req, nil
    case <-b.ctx.Done():
        return nil, io.EOF
    }
}

// Handler handles both POST (forward channel) and GET (back channel) for BrowserChannel.
type Handler struct {
    mgr      *Manager
    // listenFn is s.fs.Listen — called in a goroutine for each new session.
    listenFn func(firestorev1.Firestore_ListenServer) error
}

// NewHandler creates a BrowserChannel Handler.
func NewHandler(mgr *Manager, listenFn func(firestorev1.Firestore_ListenServer) error) *Handler {
    return &Handler{mgr: mgr, listenFn: listenFn}
}

// ServeHTTP dispatches to the POST (forward) or GET (back) channel handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    switch r.Method {
    case http.MethodPost:
        h.handleForward(w, r)
    case http.MethodGet:
        h.handleBack(w, r)
    default:
        http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
    }
}

// handleForward handles POST /Listen/channel.
// If RID is present and SID is absent, this is a new session establishment.
// If SID is present, this delivers forward-channel messages to an existing session.
func (h *Handler) handleForward(w http.ResponseWriter, r *http.Request) {
    q := r.URL.Query()
    sid := q.Get("SID")
    rid := q.Get("RID")

    if sid == "" && rid != "" {
        // New session establishment.
        sess := h.mgr.NewSession()

        // Parse the body for any initial ListenRequest messages.
        reqs, _ := parseForwardBody(r)

        ctx, cancel := context.WithCancel(r.Context())
        bridge := newListenBridge(ctx)

        // Run the gRPC Listen handler in a goroutine.
        go func() {
            defer cancel()
            defer h.mgr.Remove(sess.ID)
            _ = h.listenFn(bridge)
        }()

        // Feed initial requests.
        for _, req := range reqs {
            select {
            case bridge.recvCh <- req:
            default:
            }
        }

        // Store the bridge in the session for future forward/back channel calls.
        sess.bridge = bridge
        sess.cancel = cancel

        // Return the connection handshake.
        w.Header().Set("Content-Type", "text/plain; charset=utf-8")
        w.Header().Set("X-Content-Type-Options", "nosniff")
        w.WriteHeader(http.StatusOK)
        _, _ = w.Write(sess.FormatConnectChunk())
        return
    }

    // Forward channel message for existing session.
    sess := h.mgr.Get(sid)
    if sess == nil {
        http.Error(w, "session not found", http.StatusBadRequest)
        return
    }
    reqs, _ := parseForwardBody(r)
    for _, req := range reqs {
        select {
        case sess.bridge.recvCh <- req:
        default:
        }
    }
    w.WriteHeader(http.StatusOK)
}

// handleBack handles GET /Listen/channel — the long-polling back channel.
// It writes BrowserChannel chunks as they arrive, keeping the connection open.
func (h *Handler) handleBack(w http.ResponseWriter, r *http.Request) {
    sid := r.URL.Query().Get("SID")
    sess := h.mgr.Get(sid)
    if sess == nil {
        http.Error(w, "session not found", http.StatusBadRequest)
        return
    }

    flusher, ok := w.(http.Flusher)
    if !ok {
        http.Error(w, "streaming not supported", http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Type", "text/plain; charset=utf-8")
    w.Header().Set("Transfer-Encoding", "chunked")
    w.Header().Set("X-Content-Type-Options", "nosniff")
    w.WriteHeader(http.StatusOK)
    flusher.Flush()

    keepAlive := time.NewTicker(25 * time.Second)
    defer keepAlive.Stop()

    for {
        select {
        case <-r.Context().Done():
            return
        case <-sess.bridge.ctx.Done():
            return
        case resp, ok := <-sess.bridge.sendCh:
            if !ok {
                return
            }
            b, err := proto.Marshal(resp)
            if err != nil {
                continue
            }
            frame := EncodeGRPCWebFrame(b)
            chunk := sess.FormatDataChunk(frame)
            _, _ = w.Write(chunk)
            flusher.Flush()
        case <-keepAlive.C:
            _, _ = w.Write(sess.FormatNoopChunk())
            flusher.Flush()
        }
    }
}

// parseForwardBody decodes form-encoded BrowserChannel forward channel body.
// Body format: count=N&ofs=M&req0___data__=<base64>&req1___data__=<base64>...
func parseForwardBody(r *http.Request) ([]*firestorev1.ListenRequest, error) {
    if err := r.ParseForm(); err != nil {
        return nil, err
    }
    count := 0
    for k := range r.PostForm {
        if strings.HasSuffix(k, "___data__") {
            count++
        }
    }
    reqs := make([]*firestorev1.ListenRequest, 0, count)
    for i := 0; i < count; i++ {
        encoded := r.FormValue(fmt.Sprintf("req%d___data__", i))
        if encoded == "" {
            continue
        }
        frameBytes, err := base64.StdEncoding.DecodeString(encoded)
        if err != nil {
            continue
        }
        protoBytes, err := DecodeGRPCWebFrame(frameBytes)
        if err != nil {
            continue
        }
        req := &firestorev1.ListenRequest{}
        if err := proto.Unmarshal(protoBytes, req); err != nil {
            continue
        }
        reqs = append(reqs, req)
    }
    return reqs, nil
}
```

Add `bridge` and `cancel` fields to `Session` in `session.go`:

```go
type Session struct {
    ID     string
    mgr    *Manager
    seq    atomic.Int64
    outbox chan []byte
    // set after session establishment
    bridge *listenBridge
    cancel context.CancelFunc
}
```

Import `"fmt"` in `handler.go`.

Note: `fmt.Sprintf` is used in `parseForwardBody` but the import may need to be added. Also add the `"fmt"` import to `session.go` if not already present.

- [ ] **Step 2: Register the BrowserChannel handler in `internal/server/server.go`**

In `New()`, after creating `fs` and the registry goroutine, create the webchannel handler and register it on the mux:

```go
    import "github.com/petereon/firstyr/internal/webchannel"

    wcMgr := webchannel.NewManager()
    wcHandler := webchannel.NewHandler(wcMgr, fs.Listen)

    mux.Handle("/google.firestore.v1.Firestore/Listen/channel", wcHandler)
```

Add this before the existing `mux.Handle("/", ...)` route so the BrowserChannel path is matched first.

- [ ] **Step 3: Build and verify**

```
go build ./... 2>&1
```
Expected: clean build.

- [ ] **Step 4: Run all tests**

```
go test ./... -timeout 30s 2>&1
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/webchannel/handler.go internal/webchannel/session.go \
        internal/server/server.go
git commit -m "feat(webchannel): BrowserChannel HTTP bridge for Firebase SDK Listen RPC"
```

---

## Task 9: Update demo app to use full Firebase SDK + onSnapshot

**Files:**
- Modify: `demo-react/src/firebase.js`
- Modify: `demo-react/src/App.jsx`

- [ ] **Step 1: Update `demo-react/src/firebase.js`**

```js
import { initializeApp } from 'firebase/app';
import { getFirestore, connectFirestoreEmulator } from 'firebase/firestore';

const app = initializeApp({
  projectId: 'demo',
  apiKey: 'demo-key',
  authDomain: 'localhost',
});

export const db = getFirestore(app);

// Vite proxies /v1/* (REST) and /google.firestore.v1.Firestore/* (gRPC-Web + BrowserChannel)
// to http://localhost:17081 — firstyr handles both transports.
connectFirestoreEmulator(db, 'localhost', 5173);
```

- [ ] **Step 2: Update `demo-react/src/App.jsx` to use onSnapshot**

Replace the one-shot `getDocs` + reload pattern with a live `onSnapshot` listener. Import from `firebase/firestore` (not `/lite`).

```jsx
import { useState, useEffect } from 'react';
import {
  collection,
  addDoc,
  deleteDoc,
  doc,
  onSnapshot,
  query,
  orderBy,
} from 'firebase/firestore';
import { db } from './firebase.js';

const COLL = 'notes';

export default function App() {
  const [notes, setNotes]   = useState([]);
  const [text, setText]     = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError]   = useState(null);
  const [adding, setAdding] = useState(false);

  useEffect(() => {
    const q = query(collection(db, COLL), orderBy('createdAt', 'desc'));
    const unsub = onSnapshot(q,
      snap => {
        setNotes(snap.docs.map(d => ({ id: d.id, path: d.ref.path, ...d.data() })));
        setLoading(false);
      },
      err => setError(err.message)
    );
    return unsub;
  }, []);

  async function handleAdd(e) {
    e.preventDefault();
    if (!text.trim()) return;
    setAdding(true);
    setError(null);
    try {
      await addDoc(collection(db, COLL), {
        text: text.trim(),
        createdAt: serverTimestamp(), // uses serverTimestamp() — Plan 3 required
      });
      setText('');
    } catch (e) {
      setError(e.message);
    } finally {
      setAdding(false);
    }
  }

  async function handleDelete(id) {
    setError(null);
    try {
      await deleteDoc(doc(db, COLL, id));
    } catch (e) {
      setError(e.message);
    }
  }

  // ... JSX unchanged from Plan 2
```

Add `serverTimestamp` to the import list:
```js
import { ..., serverTimestamp } from 'firebase/firestore';
```

Note: `onSnapshot` with `orderBy` triggers the `RunQuery` handler (Plan 3 required for orderBy support). If Plan 3 is not yet complete, use `getDocs(collection(db, COLL))` instead and keep `addDoc` with `new Date().toISOString()` as the timestamp.

- [ ] **Step 3: Start the dev server and verify in the browser**

```bash
cd demo-react && bun run dev
```

Open `http://localhost:5173`. Add a note — it should appear in real-time without a page reload. Add a note in a second browser tab — it should appear in the first tab within a second.

Expected: notes appear live via `onSnapshot`; no 404 errors on `/google.firestore.v1.Firestore/Listen/channel`.

- [ ] **Step 4: Commit**

```bash
git add demo-react/src/firebase.js demo-react/src/App.jsx
git commit -m "feat(demo): use full Firebase SDK with onSnapshot live updates"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] Listen RPC bidirectional stream → Task 6
- [x] Initial snapshot delivery (all current docs + CURRENT signal) → Task 6
- [x] Live change delivery (DocChange → ListenResponse) → Task 6
- [x] NO_CHANGE resume token after each batch → Task 6
- [x] Keep-alive noops to prevent proxy timeout → Task 6
- [x] PostgreSQL LISTEN/NOTIFY → Task 3
- [x] SQLite post-write notification (WAL-level for single-process) → Task 4
- [x] Resume tokens (timestamp-based) → Task 5
- [x] BrowserChannel session management → Task 7
- [x] BrowserChannel forward channel (POST) → Task 8
- [x] BrowserChannel back channel (GET, chunked) → Task 8
- [x] gRPC-Web frame encoding/decoding → Task 7
- [x] Registry fan-out → Task 2
- [x] Demo app live updates via onSnapshot → Task 9

**Type consistency:**
- `store.DocChange` defined in Task 1, dispatched in Tasks 3/4, received in Task 6 ✓
- `listen.Registry` defined in Task 2, created in Task 6, wired in server.go (Task 8) ✓
- `listen.EncodeResumeToken` defined in Task 5, used in Task 6 ✓
- `webchannel.Manager` / `webchannel.Handler` defined in Tasks 7/8, registered in server.go ✓
- `listenBridge` fields `bridge` and `cancel` added to `Session` in Task 8 ✓

**Known limitations (acceptable for Plan 4):**
- Only `QueryTarget` targets supported; `DocumentsTarget` (single document watch) is not yet implemented — returns `Unimplemented`.
- `resume_token` reconnect replay is not yet wired; the token is sent but on reconnect a full snapshot is delivered instead of a diff. Full replay requires querying `WHERE updated_at > token_timestamp` and is a Plan 5 enhancement.
- BrowserChannel `AID` acknowledgement tracking is not implemented; messages are always delivered, never re-sent. Acceptable for local dev.
- Collection group queries in Listen are not supported.
