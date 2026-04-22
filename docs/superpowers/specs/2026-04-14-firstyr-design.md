# firstyr — Firestore Compatibility Proxy Design

**Date:** 2026-04-14
**Status:** Approved

## Overview

firstyr is a language-agnostic drop-in replacement for Google Cloud Firestore. It runs as a standalone proxy that speaks the Firestore wire protocol (gRPC + REST), backed by either SQLite or PostgreSQL. Applications change only their endpoint URL and credentials — no code changes required.

---

## Goals

- Full Firestore API fidelity: CRUD, queries, transactions, batch writes, real-time listeners, composite indexes
- Both SQLite and PostgreSQL backends, built simultaneously behind a shared adapter interface
- Both gRPC and REST protocols served from a single process
- Four configurable auth modes: `none`, `google`, `key`, `mtls`
- Deployable as a standalone binary or Docker container
- Real-time listeners via PostgreSQL `LISTEN/NOTIFY` and SQLite WAL hooks

---

## Architecture

Three layers with strict downward dependencies:

```
┌─────────────────────────────────────────────┐
│             Protocol Layer                   │
│   gRPC (google.firestore.v1 proto service)  │
│   REST (grpc-gateway HTTP/JSON transcoding) │
├─────────────────────────────────────────────┤
│             Core Engine                      │
│  Document ops · Query planner · Tx manager  │
│  Listener registry · Auth middleware         │
├─────────────────────────────────────────────┤
│           Storage Adapter Interface          │
│      SQLiteAdapter    │  PostgresAdapter     │
└──────────────────────┴──────────────────────┘
```

**Implementation language:** Go. The `grpc-gateway` library provides REST transcoding from proto annotations, eliminating the need for a separate REST implementation. `pgx` provides PostgreSQL `LISTEN/NOTIFY` support. `modernc.org/sqlite` is a pure-Go SQLite port that avoids CGo and produces a fully static binary.

The Firestore gRPC `.proto` files (`google/firestore/v1/*.proto`) are vendored into the repository and drive code generation for both the gRPC server stubs and the REST gateway. Neither the core engine nor the storage adapters import any protocol-specific types.

---

## Document Model & SQL Schema

Firestore's hierarchical document model (`collection/document/subcollection/document/...`) is flattened into a single `documents` table. The path is the primary key; collection and parent are denormalized for query efficiency.

```sql
CREATE TABLE documents (
    path        TEXT PRIMARY KEY,
    collection  TEXT NOT NULL,
    parent      TEXT,
    data        JSONB,          -- TEXT with JSON1 in SQLite
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    version     BIGINT NOT NULL DEFAULT 1
);

CREATE TABLE indexes (
    id          TEXT PRIMARY KEY,
    collection  TEXT NOT NULL,
    fields      JSONB NOT NULL,  -- [{field, order}] (TEXT with JSON1 in SQLite)
    state       TEXT NOT NULL DEFAULT 'READY'
);

CREATE TABLE transactions (
    id          TEXT PRIMARY KEY,
    started_at  TIMESTAMPTZ NOT NULL,
    reads       JSONB,
    expires_at  TIMESTAMPTZ NOT NULL
);
```

- `path` addresses every Firestore operation — no surrogate IDs.
- `collection` and `parent` are derived from `path` at write time and stored to avoid repeated string splitting during queries.
- PostgreSQL uses `JSONB` (indexable binary); SQLite uses `TEXT` with the JSON1 extension. Field access uses JSON path extraction in both cases.
- `version` supports optimistic concurrency for transactions and maps to Firestore's ETag/precondition semantics.
- Composite and single-field indexes are stored in the `indexes` table and consulted by the query planner before execution.
- Expired transaction records are swept by a background goroutine on a configurable interval.

---

## Core Engine

### Query Planner

Validates Firestore query constraints before touching the database:

- Range filters and `orderBy` must be on the same field
- Multi-field queries require a matching composite index in the `indexes` table
- Missing index → `FAILED_PRECONDITION` (not a silent wrong result)

Execution path:

```
Parsed proto query
  → Validate constraints   (missing index → FAILED_PRECONDITION)
  → Translate to SQL
      WHERE  ← field filters via JSON path extraction
      ORDER  ← orderBy fields
      LIMIT  ← limit
      WHERE  ← cursor predicates (startAt/endAfter → field value comparisons)
  → Execute
  → Deserialize rows → Firestore Document protos
```

Cursors are translated to `WHERE` predicates on the ordered fields rather than SQL `OFFSET`, matching Firestore's cursor semantics and avoiding large-offset performance degradation.

### Transaction Manager

Two transaction types:

- **Read-write:** client calls `BeginTransaction`, performs reads (paths recorded in `transactions.reads`), then calls `Commit` with writes. The proxy checks that all read documents have the same `version` as when they were read, then applies all writes atomically in a SQL transaction. Version mismatch → `ABORTED`.
- **Read-only:** served under `SERIALIZABLE` isolation (PostgreSQL) or WAL mode `BEGIN` (SQLite). No writes permitted.

`BatchWrite` applies a list of writes atomically with optional per-write `exists` preconditions — no read phase.

Transaction TTL defaults to 60 seconds (matching Firestore's limit). Configurable via `transactions.ttl`.

---

## Real-time Listeners

The Firestore `Listen` RPC is a bidirectional gRPC stream. Clients send `ListenRequest` messages to add or remove targets; the proxy pushes `ListenResponse` messages containing document changes and resume tokens.

### Listener Registry

A central in-process registry maps each target (collection path + serialized filter set) to a slice of subscriber channels:

```
Registry (sync.Map)
  └── target key
        └── []chan ListenResponse
```

Each subscriber is a goroutine blocked on its channel. Fan-out on a change: look up matching targets in the registry, send to each channel. Subscribe/unsubscribe modify the registry under a per-target mutex.

### PostgreSQL — LISTEN/NOTIFY

A SQL trigger on `documents` calls `pg_notify('doc_changes', payload)` after every `INSERT`, `UPDATE`, and `DELETE`. The payload is a JSON object containing `path`, `collection`, and `version`. The proxy holds a single persistent `LISTEN` connection via `pgx`; each notification is decoded and routed through the registry.

### SQLite — WAL Hook

SQLite's WAL commit hook fires synchronously in-process after every committed write. The hook reads the changed row and dispatches it through the same registry interface as the PostgreSQL path. The fan-out logic is identical for both backends.

### Resume Tokens

Every `ListenResponse` includes a resume token encoding the document `version` at delivery time. On reconnect with a resume token, the proxy queries `WHERE updated_at > token_timestamp` to replay missed changes before switching back to live notifications.

---

## Auth Middleware

Implemented as a gRPC interceptor (unary + streaming). Mode is selected at startup; no runtime switching. Chain order:

```
Request → Auth interceptor → Request logger → Core engine
```

**Modes:**

| Mode | Behavior |
|------|----------|
| `none` | No-op. All requests pass through. For local dev and trusted-network use. |
| `google` | Validates `Authorization: Bearer <token>` against Google's token endpoint. Checks `aud` matches configured project ID. Rejects with `UNAUTHENTICATED` on failure. |
| `key` | Validates a static API key in `Authorization: Bearer <key>` (header name configurable). Key set in proxy config. |
| `mtls` | Mutual TLS at the transport layer. Client certificate must be signed by the configured CA. |

Each mode is an independent interceptor. Adding a new mode requires adding one interceptor, not modifying existing ones.

---

## Deployment & Configuration

### Config File (YAML)

```yaml
server:
  grpc_port: 8080
  rest_port: 8081
  tls:
    cert: /etc/certs/server.crt
    key:  /etc/certs/server.key

auth:
  mode: key          # none | google | key | mtls
  key: "your-api-key"
  google_project_id: ""
  mtls_ca: ""

backend:
  type: postgres     # postgres | sqlite
  postgres:
    dsn: "postgres://user:pass@host:5432/db"
    max_conns: 25
  sqlite:
    path: /data/firestore.db

transactions:
  ttl: 60s
  sweep_interval: 30s

log:
  level: info        # debug | info | warn | error
  format: json       # json | text
```

Environment variables override config file values. All keys map to `FIRSTYR_<SECTION>_<KEY>` (e.g. `FIRSTYR_AUTH_KEY`).

### Standalone Binary

```
firstyr [--config config.yaml] [--grpc-port 8080] [--rest-port 8081] [--backend sqlite] [--auth none]
```

Flags override config file values. Built as a fully static binary (`CGO_ENABLED=0`) using `modernc.org/sqlite`.

### Docker

```dockerfile
FROM gcr.io/distroless/static
COPY firstyr /firstyr
ENTRYPOINT ["/firstyr"]
```

Config mounted as a volume or provided entirely via environment variables.

### Observability

- Structured JSON logs on every RPC: method, latency, status code, auth mode
- `/metrics` — Prometheus metrics: active listeners, query latency histograms, transaction counts, connection pool stats
- `/healthz` — liveness (process alive)
- `/readyz` — readiness (database connection established, listener goroutine running)

---

## Out of Scope (v1)

- Security rules evaluation (Firestore security rules DSL)
- Firestore emulator UI
- Multi-region replication
- Firestore import/export format compatibility
- Firebase Auth integration
