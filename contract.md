# embyr — Wire Contract

This document is the authoritative description of every request/response format embyr speaks. It covers four transports — gRPC, gRPC-Web, REST (grpc-gateway + custom interceptors), and BrowserChannel/WebChannel — and the shared semantics layered on top: documents, paths, filters, transforms, transactions, listen streams, auth.

Where the contract diverges from real Firestore (or has known limitations), the section is flagged with **⚠**. Where embyr re-derived a non-public detail (e.g. the chunk framing the SDK requires), the section is flagged with **🔍**.

---

## 1. Transports

embyr exposes the Firestore service on two TCP listeners:

- `:cfg.Server.GRPCPort` — pure gRPC over HTTP/2, no TLS by default. mTLS available via `auth.mode = mtls`.
- `:cfg.Server.RESTPort` — HTTP/1.1 multiplexed across:
  - **REST (grpc-gateway)** — JSON ↔ gRPC.
  - **gRPC-Web** — wraps the gRPC server via `improbable-eng/grpc-web/go/grpcweb`; auto-detected on the same port.
  - **BrowserChannel/WebChannel** — long-poll forward + back channel for `Listen` and `Write` streams (browsers can't speak HTTP/2 streaming reliably).
  - **Health** — `GET /healthz` and `GET /readyz`.

The REST port's request router is in <ref_file file="/Users/peter.vyboch/utilities/embyr/internal/server/server.go" />:

```
combinedHandler:
  IsGrpcWebRequest(r) || IsAcceptableGrpcCorsRequest(r) → grpcWebSrv.ServeHTTP
  HasSuffix(path, "/channel")                          → restMux (no write deadline)
  otherwise                                            → http.TimeoutHandler(restMux, 30s)

restMux:
  GET  /healthz             → 200 "ok"
  GET  /readyz              → 200 if db.Ping ok, 503 otherwise
  POST /…/Listen/channel    → webchannel.Handler
  POST /…/Write/channel     → webchannel.WriteHandler
  POST */documents:runQuery, *:batchGet, *:runAggregationQuery
                            → custom JSON-array streamers
  *                         → grpc-gateway runtime mux
```

### CORS

```go
AllowedMethods:   {GET, POST, PUT, PATCH, DELETE, OPTIONS}
AllowedHeaders:   {*}
AllowCredentials: true
AllowedOrigins:   cfg.Server.AllowedOrigins   // empty list = allow all (dev default)
```

### Timeouts

- `ReadHeaderTimeout` 10 s, `ReadTimeout` 30 s — applied to every request.
- `WriteTimeout` 0 — disabled at the server level.
- A 30 s `http.TimeoutHandler` wraps the REST mux **except** for paths matched by the `combinedHandler` early-out (gRPC-Web, `*/channel`).
- Streaming RPCs (`RunQuery`, `Listen`, `Write`, BrowserChannel back-channel) hold connections open indefinitely; only the request body read enforces 30 s.

### gRPC code → HTTP status (`grpcCodeToHTTP` in server.go)

| gRPC code | HTTP |
|---|---|
| `NotFound` | 404 |
| `AlreadyExists` | 409 |
| `InvalidArgument` | 400 |
| `Unauthenticated` | 401 |
| `PermissionDenied` | 403 |
| `Unimplemented` | 501 |
| anything else | 500 |

This mapping is used by the custom JSON-array streamers and the WebChannel handlers; grpc-gateway uses its own internal mapping for routes it owns.

---

## 2. Documents and paths

### Resource name

```
projects/{project}/databases/{database}/documents/{collection}/{docId}[/{coll}/{doc}]*
```

`{database}` is typically `(default)`. The number of segments after `documents/` must be even. `store.ParsePath` splits the trailing collection/doc pair off as `(collection, parent)`.

### Document encoding

Stored row layout (`store.Document` in <ref_file file="/Users/peter.vyboch/utilities/embyr/internal/store/doc.go" />):

| field | type | meaning |
|---|---|---|
| `path` | text | full resource name, primary key |
| `collection` | text | trailing segment derived from `path` |
| `parent` | text | parent path derived from `path` |
| `data` | jsonb / text | proto3-JSON of `{"fields":{...}}` |
| `created_at` | timestamptz / RFC3339Nano text | first write |
| `updated_at` | timestamptz / RFC3339Nano text | latest write |
| `version` | int8 | OCC counter, +1 per write |

`data` always serializes the fields-only sub-message (`codec.ProtoToStore`). Empty docs are stored as `"{}"` and decoded back to a `Fields=nil` document by `codec.StoreToProto`.

### Field paths and dot-notation

Field paths are validated against `^[a-zA-Z_][a-zA-Z0-9_.]*$` (`validateFieldPath` in the SQL layers) before any `fmt.Sprintf` interpolation. Dot-notation traverses nested `MapValue.fields`:

```
field path "profile.age" maps to JSON path
  $.fields.profile.mapValue.fields.age
```

Both the SQL layer (`sqliteFieldBase`/`pgFieldBase`) and the in-memory live-update matcher (`codec.lookupNestedField`) use this exact traversal.

### Document ID generation (auto IDs)

`codec.NewDocumentID` returns 20-character strings drawn from a 62-character alphabet (`a-zA-Z0-9`), using `crypto/rand` with rejection sampling at byte ≥ 248 to keep `byte % 62` uniform. Returns `(string, error)` — propagated as `codes.Internal` if entropy fails.

---

## 3. Values

`Value` proto (REST/proto3-JSON) variants embyr understands:

| variant | JSON shape (REST) | store.FilterValueKind |
|---|---|---|
| `nullValue` | `{"nullValue": null}` | `FilterValueNull` |
| `booleanValue` | `{"booleanValue": true}` | `FilterValueBool` |
| `integerValue` | `{"integerValue": "42"}` *(string per proto3-JSON)* | `FilterValueInt` |
| `doubleValue` | `{"doubleValue": 3.14}` | `FilterValueDouble` |
| `stringValue` | `{"stringValue": "x"}` | `FilterValueString` |
| `timestampValue` | `{"timestampValue": "2026-04-25T20:36:30.382807Z"}` | `FilterValueTime` |
| `arrayValue` | `{"arrayValue":{"values":[…]}}` | `FilterValueArray` (filter operands only) |
| `mapValue` | `{"mapValue":{"fields":{…}}}` | serialized as `FilterValueJSON` for compare |

Cross-type numeric compare: `int` ↔ `double` are compared after promoting to a common form. The in-memory matcher uses `int64` directly when both sides are `int`. The Postgres SQL layer uses `COALESCE((…->>'integerValue')::float8, (…->>'doubleValue')::float8)` for cross-type comparisons. SQLite uses `CAST(…integerValue AS INTEGER)` to preserve precision past 2^53.

---

## 4. gRPC service `google.firestore.v1.Firestore`

All RPCs are implemented in <ref_file file="/Users/peter.vyboch/utilities/embyr/internal/server/handlers.go" />, <ref_file file="/Users/peter.vyboch/utilities/embyr/internal/server/transactions.go" />, <ref_file file="/Users/peter.vyboch/utilities/embyr/internal/server/listen.go" />, and <ref_file file="/Users/peter.vyboch/utilities/embyr/internal/server/aggregation.go" />.

### 4.1 Unary RPCs

| RPC | Required input | Output | Errors |
|---|---|---|---|
| `GetDocument(name)` | `name` | `Document` | `InvalidArgument`, `NotFound` |
| `CreateDocument(parent, collection_id, document_id?, document)` | `parent`, `collection_id` | created `Document` | `InvalidArgument`, `AlreadyExists`, `Internal` |
| `UpdateDocument(document, update_mask?, current_document?)` | `document.name` | updated `Document` | `InvalidArgument`, `NotFound` (Update-mode), `AlreadyExists` (InsertOnly), `FailedPrecondition` (`update_time`) |
| `DeleteDocument(name, current_document?)` | `name` | `Empty` | `InvalidArgument`, `NotFound` (when `exists=true`) |
| `ListDocuments(parent, collection_id?, page_size?, page_token?)` | `parent` | `{documents, next_page_token}` | `InvalidArgument` |
| `ListCollectionIds(parent, page_size?, page_token?)` | `parent` | `{collection_ids, next_page_token}` | `InvalidArgument` |
| `BeginTransaction(options?)` | — | `{transaction}` | — |
| `Rollback(transaction)` | `transaction` | `Empty` | `InvalidArgument` |
| `Commit(writes, transaction?)` | — | `{write_results, commit_time}` | see §4.5 |
| `BatchWrite(writes)` | — | `{write_results, status}` | per-write status, no top-level error |

### 4.2 Server-streaming RPCs

| RPC | Output stream |
|---|---|
| `RunQuery(parent, structured_query)` | `RunQueryResponse{document, read_time}…`, then `RunQueryResponse{continuation_selector: {done: true}, read_time}` |
| `RunAggregationQuery(parent, structured_aggregation_query)` | exactly one `RunAggregationQueryResponse{result: {aggregate_fields}, read_time}` |
| `BatchGetDocuments(documents, consistency_selector?)` | one `BatchGetDocumentsResponse` per requested path; `Found` or `Missing`; if `new_transaction` is requested, the **first** message carries `transaction` and no result |

### 4.3 Bidi-streaming RPCs

`Listen` — see §6.

`Write` — three-step protocol (<ref_snippet file="/Users/peter.vyboch/utilities/embyr/internal/server/handlers.go" lines="412-474" />):

1. Client sends a handshake `WriteRequest` (empty `writes`, empty `stream_id`).
2. Server replies with a `WriteResponse{stream_id: <16-hex unixnano>, stream_token: <RFC3339Nano>, commit_time}`. No `write_results`.
3. Client sends `WriteRequest{writes:[…], stream_id, stream_token}`. Server replies with `WriteResponse{stream_id, stream_token: <new>, write_results, commit_time}`. Each batch is applied inside `WithTransaction`.

EOF on `Recv` returns nil; `codes.Canceled` returns nil; any other `Recv`/`Send` error terminates the stream and the SDK reconnects.

### 4.4 Field transforms (`update_transforms`)

Implemented in `applyFieldTransforms` (handlers.go):

| transform | input | result on missing field |
|---|---|---|
| `setToServerValue: REQUEST_TIME` | — | sets to `now` UTC |
| `increment: integerValue \| doubleValue` | numeric delta | `existing = 0`; type follows delta type |
| `appendMissingElements: ArrayValue` | values to add | dedup-merge with current array using `proto.Equal` |
| `removeAllFromArray: ArrayValue` | values to remove | no-op if field absent; otherwise filtered list |

Increment with a non-numeric delta returns `InvalidArgument`. ⚠ `setToServerValue` other than `REQUEST_TIME` is silently ignored (and a `nil` `transformResults[i]` is appended; the gRPC layer normalizes that to an empty `Value`).

`WriteResult.transform_results` is one entry per transform, in input order.

### 4.5 `Commit` — non-tx vs tx paths

Both paths preserve the **same WriteResults length contract**: exactly one `WriteResult` per `Write`, including `VerifyMutation` entries (Firestore SDK throws on mismatch).

**Non-tx path** (`req.transaction` empty): runs `applyWriteBatch` inside `WithTransaction`. Each `Write`:
- `update`: see §4.6.
- `delete`: see §4.6.
- `nil` Operation (`VerifyMutation`): appends a `WriteResult{update_time: commit_time}`. ⚠ The proto generated in `gen/` has no `verify` field, so the precondition cannot be checked against a target — only the length contract is honored.

**Tx path** (`req.transaction` set): converts writes to `store.WriteOp` records and calls `store.CommitTransaction`. The storage layer:
1. Loads the transaction's recorded read set.
2. Begins a SQL transaction.
3. For each recorded `(path, version)`: re-reads the row's `version`. Mismatch → rollback and `codes.Aborted`. Missing row → rollback and `codes.Aborted "<path> was deleted"`.
4. Applies each `WriteOp`.
5. Deletes the transaction record.
6. Commits.
7. Fans out `DocChange` events to subscribers.

VerifyMutation entries in the tx path produce `WriteResult{update_time: commit_time}`; real ops get their real `updated_at`.

### 4.6 Per-write semantics (`applyWriteBatch`)

For each `Write`:

```
type           default mode    precondition → mode override
─────────────  ──────────────  ─────────────────────────────────
update          Upsert         exists=true        → Update
                               exists=false       → InsertOnly
                               update_time set    → Update + version check
delete          Upsert (n/a)   exists=true        → mustExist=true
                               update_time set    → mustExist=true
nil (verify)    no op          (length contract only — no actual check)
```

Update writes read the current document **only when** any of `update_mask`, `update_transforms`, or `update_time` precondition is present. The read serves three purposes simultaneously: to validate `update_time` (microsecond precision via `time.Truncate(time.Microsecond)`), to provide the base for transforms, and to provide the base for mask-merge.

Mask alone does **not** force `Update` mode (`setDoc({merge:true})` sends a mask without a precondition; semantics is "merge-or-create"). Only an explicit `exists=true` precondition switches to `Update`.

Mask paths use dot-notation; siblings outside the masked subtree are preserved (`applyMask` walks recursively, deep-copying intermediate `MapValue` nodes to avoid aliasing).

Transform-only writes (no mask, no fields) seed transforms from the current document so siblings survive: e.g. `updateDoc({"meta.updatedAt": serverTimestamp()})` keeps `meta.name`.

### 4.7 `BatchWrite`

Per-write isolation: each `Write` runs in its **own** `WithTransaction`. A failure does not roll back its siblings. Output:

```
{
  write_results: [WriteResult{}, WriteResult{}, …]  // one per Write, aligned
  status:        [null|google.rpc.Status, …]        // null = OK
}
```

Per-write semantics are identical to `Commit` non-tx (mask, transforms, preconditions all honored).

### 4.8 Filter operators

`store.FilterOp` enum defined in <ref_file file="/Users/peter.vyboch/utilities/embyr/internal/store/query.go" />:

```
==  !=  <  <=  >  >=
in  not-in
array-contains   array-contains-any
```

Composite filter is AND-only. ⚠ `OR` composite is `Unimplemented`.
Unary filters: `IS_NULL` and `IS_NOT_NULL` translate to `==`/`!=` against `FilterValueNull` operand.

### 4.9 OrderBy + cursors

`structured_query.order_by` produces `[]store.OrderBy{Field, Direction (ASC|DESC)}`. For non-scalar fields, ordering uses `COALESCE` over the typed sub-fields so numeric values sort numerically and string/timestamp sort lexicographically.

Cursors (`startAt`/`startAfter`/`endAt`/`endBefore`) decoded into `store.Cursor{Values, Before, IsEnd}`:
- `startAt(v)` → `Before=true,  IsEnd=false` → `field >= v`  (or `<=` desc)
- `startAfter(v)` → `Before=false, IsEnd=false` → `field >  v`  (or `<` desc)
- `endAt(v)` → `Before=false, IsEnd=true`  → `field <= v`  (or `>=` desc)
- `endBefore(v)` → `Before=true,  IsEnd=true`  → `field <  v`  (or `>` desc)

Multi-field cursors expand to lexicographic OR-of-AND form.

### 4.10 Pagination

Page tokens are opaque; format is `base64.RawURLEncoding(decimal_offset)`. `store.DecodePageToken` returns 0 on empty/invalid input. When a query has cursors, page tokens are not used (offset stays 0). Look-ahead pagination: server fetches `pageSize+1` rows, returns `pageSize` and emits a `next_page_token` only if more were available.

---

## 5. REST (HTTP/JSON via grpc-gateway + custom interceptors)

The grpc-gateway mux serves the standard Firestore REST mappings. embyr **intercepts** three of those paths because the JS SDK expects a JSON-array body that grpc-gateway's NDJSON streaming wouldn't produce.

### 5.1 grpc-gateway-served routes

URL templates from <ref_file file="/Users/peter.vyboch/utilities/embyr/gen/go/google/firestore/v1/firestore.pb.gw.go" />:

| Method | URL | RPC |
|---|---|---|
| GET | `/v1/{name=projects/*/databases/*/documents/*/**}` | `GetDocument` |
| GET | `/v1/{parent=projects/*/databases/*/documents}/{collection_id}` | `ListDocuments` |
| GET | `/v1/{parent=projects/*/databases/*/documents/*/**}/{collection_id}` | `ListDocuments` |
| POST | `/v1/{parent=projects/*/databases/*/documents}/{collection_id}` | `CreateDocument` |
| POST | `/v1/{parent=projects/*/databases/*/documents/*/**}/{collection_id}` | `CreateDocument` |
| PATCH | `/v1/{document.name=projects/*/databases/*/documents/*/**}` | `UpdateDocument` |
| DELETE | `/v1/{name=projects/*/databases/*/documents/*/**}` | `DeleteDocument` |
| POST | `/v1/{database=projects/*/databases/*}/documents:beginTransaction` | `BeginTransaction` |
| POST | `/v1/{database=projects/*/databases/*}/documents:commit` | `Commit` |
| POST | `/v1/{database=projects/*/databases/*}/documents:rollback` | `Rollback` |
| POST | `/v1/{database=projects/*/databases/*}/documents:batchWrite` | `BatchWrite` |
| POST | `/v1/{parent=projects/*/databases/*/documents}:listCollectionIds` | `ListCollectionIds` |
| POST | `/v1/{parent=projects/*/databases/*/documents/*/**}:listCollectionIds` | `ListCollectionIds` |

Bodies and responses use proto3-JSON (field names lowerCamelCase). grpc-gateway emits gRPC status as standard HTTP error.

### 5.2 Intercepted JSON-array routes

These are matched in the catch-all `/` handler before grpc-gateway sees them:

#### `POST …/documents:runQuery` and `POST …/documents/{parent}:runQuery`

- Request body: proto3-JSON `RunQueryRequest`. `parent` is taken from the URL (override of any value in the body).
- Response: streaming JSON array. The first byte is `[`, items are comma-separated, the final byte is `]`. Each item is a proto3-JSON `RunQueryResponse`, including the trailing `{"continuationSelector":{"done":true},"readTime":…}`. `Content-Type: application/json`. `http.Flusher.Flush()` is called after every item — bytes leave the server as soon as each document is ready.
- Error handling:
  - **Before any byte was written:** plain `http.Error` with `grpcCodeToHTTP` status.
  - **Mid-stream:** appends `,{"error":{"code":<HTTP_CODE>,"message":<msg>,"status":<GRPC_NAME>}}]` and closes the array.

#### `POST …/documents:batchGet`

- Request body: proto3-JSON `BatchGetDocumentsRequest`. `database` is taken from the URL.
- Response: a single fully-buffered JSON array of `BatchGetDocumentsResponse` items. `Content-Type: application/json`. ⚠ Not streamed — the JS Lite SDK uses `Array.prototype.forEach` on the parsed body.
- Each item is one of: `{"transaction":"…","readTime":…}` (only when `new_transaction` was requested; sent first), `{"found":Document, "readTime":…}`, or `{"missing":path, "readTime":…}`.

#### `POST …/documents:runAggregationQuery` and `POST …/documents/{parent}:runAggregationQuery`

- Request body: proto3-JSON `RunAggregationQueryRequest`.
- Response: a single JSON array, either `[]` or `[<RunAggregationQueryResponse>]`. `Content-Type: application/json`.

In all three: errors before the first byte are HTTP errors with `grpcCodeToHTTP` mapping; for `:runQuery` only, errors after first-byte are inlined as the array's last element.

---

## 6. `Listen` bidirectional stream

The full RPC-level protocol; the BrowserChannel section (§7) describes how this is tunneled over HTTP for browsers.

### 6.1 Client → server (`ListenRequest`)

Two variants per request:

```
{
  "database": "projects/p/databases/(default)",
  "addTarget": {
    "targetId": <int32>,
    "query":  {parent, structuredQuery}      // OR
    "documents": {documents: [path, …]}
  }
}

{
  "database": "projects/p/databases/(default)",
  "removeTarget": <int32>
}
```

- `targetId` is client-chosen and globally unique within the stream.
- Re-adding an existing target ID is treated as "remove + re-add" — the server sends `TargetChange{REMOVE, [id]}` first, then `ADD`, then a fresh snapshot. (This matches the real emulator; it does not raise `AlreadyExists`.)

### 6.2 Server → client (`ListenResponse`)

Five message variants:

```
{"targetChange": {"targetChangeType": ADD,        "targetIds":[id], "readTime":…}}
{"targetChange": {"targetChangeType": REMOVE,     "targetIds":[id], "readTime":…}}
{"targetChange": {"targetChangeType": CURRENT,    "targetIds":[id], "readTime":…, "resumeToken": "<base64>"}}
{"targetChange": {"targetChangeType": NO_CHANGE,  "readTime":…,                    "resumeToken"?: "<base64>"}}
{"targetChange": {"targetChangeType": RESET,      "targetIds":[id, …], "readTime":…}}

{"documentChange": {"document": Document, "targetIds":[id, …]}}
{"documentDelete": {"document": path, "removedTargetIds":[id, …], "readTime":…}}
```

Resume tokens are `base64.RawURLEncoding(time.UTC().Format(RFC3339Nano))` (`internal/listen/token.go`). The token's only meaning to embyr is "this is the read-time"; it is not used for replay. ⚠ Resume from a token is not implemented; clients reconnecting do a full re-snapshot.

### 6.3 Snapshot delivery (per `addTarget`)

```
1. ADD target_change      (no resume token)
2. for each matching doc: documentChange{document, targetIds:[id]}
3. CURRENT target_change  (resume_token = read_time at step 1)
4. NO_CHANGE              (resume_token = now)   ← seals the snapshot
```

Step 4 is what the JS SDK waits for before resolving `getDoc()` / firing `onSnapshot()` for the first time.

For `documents` targets, missing paths are silently skipped. For `query` targets, the server pages through `QueryDocuments` until exhausted (`pageSize` 300 by default, or the explicit `limit`). Read-time is captured **before** the first query page.

### 6.4 Live changes

After the snapshot, every committed write reaches the stream via `listen.Registry.SubscribeWithSignal`. For each `DocChange`:

- If the registry's per-subscriber buffer overflowed since the last call to `Overflowed()`: send `RESET` for every active target ID, then re-deliver the full snapshot for each, then continue with the current event.
- Match the change against every active target:
  - `documents` target: exact path match.
  - `query` target: `parent + collection` match, then in-memory filter check via `codec.MatchesFilter` (which understands dot-notation and the same operators as the SQL layer, with int64 precision preserved).
- Emit `documentChange` (upsert; server re-fetches the doc to get the latest version) or `documentDelete`.
- After every delivered change, send a `NO_CHANGE` with a fresh resume token.

### 6.5 Keep-alive

Every 30 s of idle, the server emits a bare `targetChange{NO_CHANGE}` (no resume token, no targets). The 25 s ticker on the WebChannel back-channel side (§7.4) is independent of this 30 s `ListenResponse` ticker.

### 6.6 Termination

`stream.Recv` errors:
- `io.EOF` → return nil (client closed cleanly).
- anything else → return the error.

The recv goroutine and main loop both respect `ctx.Done()` so a cancelled stream cannot leak the recv goroutine even when the recv buffer is full.

---

## 7. WebChannel / BrowserChannel transport

🔍 Implements the protocol the Firebase JS SDK expects when running in a browser. There is no public spec; the contract below is reverse-engineered from the JS SDK's parser (`@firebase/webchannel-wrapper`) and confirmed by black-box testing.

### 7.1 URL paths

```
/google.firestore.v1.Firestore/Listen/channel
/google.firestore.v1.Firestore/Write/channel
```

Both paths accept `POST` (forward channel) and `GET` (back channel).

### 7.2 Query parameters

| param | direction | meaning |
|---|---|---|
| `VER` | both | protocol version (always 8); not checked server-side |
| `database` | both | `projects/.../databases/...`; not checked server-side |
| `RID` | POST | request id; SDK retries with the same RID; server dedups via a 16-entry per-session ring buffer |
| `SID` | POST and GET | session id (24-hex); absent on the new-session POST |
| `AID` | GET | last array-id the client has acknowledged; for AID ≥ 2, log index = AID − 1 |
| `CI` | GET | connection index (0, 1, …); only used for logging |
| `TYPE` | GET | `"xmlhttp"` for the back-channel (not checked server-side) |
| `RID=rpc` | GET | sentinel value the SDK uses for the back-channel "request" |

### 7.3 Forward channel — `POST /…/channel`

Request body is form-encoded (`application/x-www-form-urlencoded`):

```
count=<N>&ofs=<M>&req0___data__=<proto3-JSON>&req1___data__=<proto3-JSON>&…
```

Each `reqN___data__` is a proto3-JSON-encoded message — `ListenRequest` for `Listen/channel`, `WriteRequest` for `Write/channel`. `count` is N; `ofs` is unused server-side.

#### 7.3.1 New-session POST (no `SID`)

The server creates a `Session`, spawns the gRPC handler goroutine, spawns the pump goroutine, pushes the parsed reqs to the bridge's `recvCh`, and returns the **connect chunk** (§7.5) as the response body.

Response headers:
```
Content-Type: text/plain; charset=utf-8
X-Content-Type-Options: nosniff
```
Status: 200.

The connect chunk's session id is what the SDK parses out and uses as `SID` for all subsequent requests.

#### 7.3.2 Existing-session POST

```
sess := mgr.Get(SID)
if sess == nil || sess.GetListenBridge() == nil { return 400 "session not found" }
if !sess.SeenRID(RID) {
    parse body and push each request to bridge.recvCh
}
write formatForwardPostStatus(sess)
```

Cross-handler safety: a Listen POST whose SID belongs to a Write session (or vice versa) is rejected with 400. `Session.GetListenBridge()` and `GetWriteBridge()` return nil for the wrong kind.

🔍 **Response body is a chunk-framed 3-element JSON array:**

```
<byte-length>\n[<lastArrayIdSent>,0,0]
```

Example: after the connect chunk and 2 data chunks, `s.seq=4`, body = `7\n[3,0,0]`.

The SDK applies the same chunk extractor (`Sb` in `webchannel-wrapper`) to forward POST responses as it does to the back-channel. A bare JSON body or an empty body leaves the SDK in "incomplete chunk" state, which marks the forward request failed and blocks every subsequent send on the session — observed as filter-switching that hangs after the first switch and full session reconnects every ~10 s. The format `<len>\n<json>` with a 3-element array is what the SDK requires; the precise meaning of elements 2 and 3 (here `0,0` for "outstandingBytes" and "unused") is not validated by the SDK.

`Content-Type: text/plain; charset=utf-8`. Status: 200.

### 7.4 Back channel — `GET /…/channel?SID=…&AID=…&CI=…`

Long-poll streaming response. Response headers:

```
Content-Type:           text/plain; charset=utf-8
Transfer-Encoding:      chunked
X-Content-Type-Options: nosniff
```

Status: 200. The handler `Flush()`es the headers, then loops:

```
for {
    chunks, notify := sess.{listen|write}Log.From(logIdx)
    write each chunk; flush; logIdx += len(chunks)
    select {
        <-r.Context().Done():     // client gone
        <-bridge.ctx.Done():      // session gone — drain remaining and return
        <-notify:                 // pump appended new data
        <-keepAlive.C:            // 25 s
            write FormatNoopChunk(NextNoopSeq())
            flush
    }
}
```

`logIdx = max(0, AID − 1)`. Each session has two append-only `MsgLog`s (`listenLog`, `writeLog`); the pump goroutine reads `*Response` from `bridge.sendCh`, JSON-marshals, and `Append`s to the relevant log. `MsgLog.From(i)` returns the slice from index `i` plus a `ready` chan that closes when the next `Append` happens — the loop can wait without polling.

Cross-handler safety on GET: same nil-bridge check as POST → 400.

### 7.5 Wire chunk formats

Each chunk on either the new-session POST response body or the back-channel stream is:

```
<decimal byte length>\n<UTF-8 JSON>
```

Multiple chunks concatenate without delimiters. The JSON is **always** an array of `[seq, payload]` pairs. embyr emits exactly one pair per chunk except for the connect chunk which emits two.

#### Connect chunk (`Session.FormatConnectChunk`)

```
59
[[0,["c","<sid>","",8,8,0]],[1,["noop"]]]
```

(`<sid>` is the 24-hex session ID; the trailing `8,8,0` are protocol params expected by the SDK; the `["noop"]` keeps the array's array-id rolling.)

After this, `Session.seq = 2` (the next data chunk will use seq=2).

#### Data chunk (`Session.FormatDataChunk`)

```
N
[[<seq>,[<json-message>]]]
```

`<json-message>` is `protojson.Marshal(*ListenResponse)` or `protojson.Marshal(*WriteResponse)`. The double bracket around the message — `[<json>]` — is **required** by the SDK's dispatcher (`Rb` extracts `t.data[0]` as the message; the wrapping array signals "data carries a single message object"). Removing the wrap triggers `INTERNAL ASSERTION FAILED: Unexpected state` in the SDK.

`seq` is claimed via `s.seq.Add(1) - 1`, so it's monotonic and globally unique.

#### Noop chunk (`Session.FormatNoopChunk(seq)`)

```
N
[[<seq>,["noop"]]]
```

`seq` is claimed via `Session.NextNoopSeq()` which uses the **same** atomic counter as data chunks, guaranteeing no two chunks in a session ever share a seq — even with multiple concurrent back-channel connections (CI=0, CI=1, …).

### 7.6 RID dedup

`Session.SeenRID(rid)` keeps a slice of the last `ridWindowSize = 16` RIDs. If the RID is in the buffer, the request body is silently skipped (but the response still includes the forward POST status body so the SDK's state machine advances). RIDs older than 16 fall out of the window.

### 7.7 Manager.Shutdown

The session bridge contexts are derived from `context.Background()` so that the underlying gRPC handler can survive an HTTP request boundary. To prevent leaking goroutines past server shutdown, `Manager.Shutdown` is called from the egCtx-watching goroutine in `Server.Run`; it cancels every active session's bridge ctx, which causes the gRPC handler to return and the pump to exit.

---

## 8. Authentication

Configured via `cfg.Auth.Mode` (<ref_file file="/Users/peter.vyboch/utilities/embyr/internal/auth/interceptor.go" />). Applied as gRPC unary + stream interceptors; gRPC-Web inherits the same. REST and WebChannel are **not** interceptor-protected — they sit behind the same `:RESTPort` listener with no per-request auth check (gRPC-Web requests do go through the gRPC interceptor since they hit the wrapped server).

| mode | required config | how the bearer is taken | what's validated |
|---|---|---|---|
| `none`, `""` | — | — | nothing |
| `key` | `auth.key` | gRPC metadata `authorization: Bearer <key>` | constant-time `==` against `cfg.Auth.Key` |
| `google` | `auth.google_project_id` | gRPC metadata `authorization: Bearer <token>` | calls Google's `tokeninfo` endpoint with `?id_token=…` first; on 4xx falls back to `?access_token=…`. ID-token path requires `audience` / `aud` / `azp` to equal `cfg.Auth.GoogleProjectID`. Access-token success returns OK regardless of project (response shape doesn't carry project id). |
| `mtls` | `auth.mtls_ca`, `server.tls.cert`, `server.tls.key` | TLS peer | requires `state.VerifiedChains` non-empty (any client cert signed by `auth.mtls_ca` is accepted; embyr does not check Subject/CN). |

Errors are `codes.Unauthenticated`. Failure to construct credentials at startup (missing CA, missing key, etc.) returns from `auth.New` and prevents the server from starting.

The `tokeninfo` URL is overridable via `auth.SetTokenInfoURL` for tests.

---

## 9. Configuration

Loaded by `internal/config.Load` from a YAML file (path via `-config` flag) plus env vars (`EMBYR_*`). Validated by `(*Config).Validate`.

```yaml
server:
  grpc_port: 8080            # 1..65535, must differ from rest_port
  rest_port: 8081            # 1..65535
  allowed_origins: []         # empty = allow all (dev); non-empty restricts CORS
  tls:
    cert: ""                  # PEM path; required for mtls mode
    key:  ""

auth:
  mode: "none"                # one of: none, key, google, mtls
  key: ""                     # required when mode=key
  google_project_id: ""       # required when mode=google
  mtls_ca: ""                 # required when mode=mtls

backend:
  type: "sqlite"              # sqlite | postgres
  sqlite:
    path: "embyr.db"
  postgres:
    dsn: ""                   # postgres://user:pass@host:port/db?sslmode=…
    max_conns: 25             # 0 = sql package default

transactions:
  ttl: 60s                    # >0; per-Firestore-tx lifetime; threaded through Adapter.SetTransactionTTL
  sweep_interval: 30s         # >0; how often the sweeper deletes expired txs

log:
  level: "info"
  format: "json"              # json | <anything else> = development
```

Env-var binding uses lowerCamelCase → SCREAMING_SNAKE: `EMBYR_SERVER_GRPC_PORT`, `EMBYR_AUTH_KEY`, `EMBYR_BACKEND_POSTGRES_DSN`, etc.

---

## 10. Storage backend contract

A `store.StorageAdapter` is one of `sqlite.Adapter` or `postgres.Adapter`. Both implement the same interface.

### 10.1 Schema

```sql
documents(
  path        TEXT PRIMARY KEY,
  collection  TEXT NOT NULL,
  parent      TEXT,
  data        JSONB / TEXT,         -- proto3-JSON {"fields":{...}}
  created_at  TIMESTAMPTZ / RFC3339Nano,
  updated_at  TIMESTAMPTZ / RFC3339Nano,
  version     BIGINT NOT NULL DEFAULT 1
);

transactions(
  id          TEXT PRIMARY KEY,
  started_at  TIMESTAMPTZ / RFC3339Nano,
  reads       JSONB / TEXT,         -- {"<path>": <version>, ...}
  expires_at  TIMESTAMPTZ / RFC3339Nano
);
```

Postgres has GIN index on `data`, BTREE on `collection`, `parent`, `updated_at`. Postgres also has the `notify_doc_change` trigger that fires `pg_notify('doc_changes', payload)` on every documents row change; embyr `Subscribe` listens to that channel.

### 10.2 OCC contract

1. `BeginTransaction(readOnly)` → 128-bit hex tx id, `expires_at = now + cfg.Transactions.TTL`.
2. `GetDocumentForTransaction(txID, path)` → returns the doc and records `(path, doc.Version)` into the tx's `reads` map.
3. `CommitTransaction(txID, ops)`:
   - reads the recorded `reads` map outside any tx;
   - opens a new SQL tx;
   - for each `(path, version)`: re-reads `documents.version`; mismatch → `codes.Aborted "version mismatch"`; missing → `codes.Aborted "was deleted"`;
   - applies each `WriteOp`;
   - deletes the tx record;
   - commits;
   - notifies subscribers.
4. `RollbackTransaction(txID)` → deletes the tx record; no version checks.
5. `SweepExpiredTransactions()` → deletes rows with `expires_at < now`; called every `cfg.Transactions.SweepInterval` from a background goroutine in `Server.Run`.

`WithTransaction(ctx, fn)` runs `fn` inside an SQL tx, with `defer` rollback on panic — so a panic in user code never leaks the tx (critical for SQLite where `MaxOpenConns=1` would deadlock).

### 10.3 Subscribe / changefeed

`StorageAdapter.Subscribe(ctx)` returns `(<-chan DocChange, cancel)`. SQLite drives the channel from in-process `notifySubscribers` calls inside CRUD methods. Postgres drives it from `LISTEN doc_changes` on a dedicated pgx connection with auto-reconnect (1 s → 30 s exponential backoff).

`DocChange`:
```go
{Path, Collection, Parent string, Kind: Upsert|Delete, Version int64, Data string}
```

`Data` carries the `documents.data` JSON for upserts; the Listen handler currently ignores it and re-fetches via `GetDocument` (because the trigger payload has an 8000-byte cap and may have been truncated to empty).

The `internal/listen.Registry` fans `DocChange` out to per-Listen-stream subscribers. Each subscriber has a 64-buffered channel and a sticky `Overflowed` flag that the Listen handler consumes (and clears) before each fanout — when set, the handler emits a `RESET` for every active target and re-snapshots.

### 10.4 SQLite specifics

- `journal_mode = WAL` enforced at startup (fail-fast if not).
- `busy_timeout = 5000ms`.
- `MaxOpenConns(1)` — SQLite is single-writer; a single connection guarantees no driver-level race on the write lock. Reads and writes serialize.
- All CRUD methods use `execer(ctx)` which returns the in-context `*sql.Tx` if present, otherwise the shared `*sql.DB`.

### 10.5 Postgres specifics

- `MaxOpenConns(cfg.Backend.Postgres.MaxConns)` (0 = unlimited, default 25 from config defaults).
- DSN held in a closure (`connect func(ctx) (*pgx.Conn, error)`), not in a named struct field — so `%+v` and reflection don't expose the password.

---

## 11. Health

```
GET /healthz → 200 "ok"                         (always, as long as the process is up)
GET /readyz  → 200 "ok"   if db.Ping(ctx, 2s) is nil
              → 503 with body "db unreachable: <err>"
```

No auth, no CORS prefix matching, no timeout.
