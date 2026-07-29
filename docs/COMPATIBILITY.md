# Embyr — Firestore API Compatibility & Feature Matrix

Embyr implements the `google.firestore.v1.Firestore` gRPC service specification and wire protocol. This document outlines implemented RPCs, query capabilities, transport support, and known limitations compared to Google Cloud Firestore.

---

## 📡 Transport Support

Embyr exposes Firestore on four protocol transports:

| Transport | Protocol | Endpoint / Port | Use Case |
| :--- | :--- | :--- | :--- |
| **gRPC** | HTTP/2 binary gRPC | `:8080` (gRPC Port) | Native backend SDKs (Go, Node.js, Python, Java). |
| **gRPC-Web** | HTTP/1.1 gRPC-Web framing | `:8081` (REST Port) | Web clients using `grpc-web` JS libraries. |
| **WebChannel** | BrowserChannel long-polling | `:8081` (`/channel`) | Firebase Web SDK in browser environments (Listen & Write streams). |
| **REST** | HTTP/1.1 JSON (grpc-gateway) | `:8081` (REST Port) | Standard HTTP REST API calls and custom JSON streamers. |

---

## 🟢 Implemented Firestore v1 RPCs

| Method Name | Standard Endpoint | Status | Description |
| :--- | :--- | :---: | :--- |
| `GetDocument` | `GET /v1/{name=projects/*/databases/*/documents/*/*}` | ✅ Implemented | Retrieves a single document by resource path. |
| `ListDocuments` | `GET /v1/{parent=...}/documents/{collection_id}` | ✅ Implemented | Lists documents within a collection with pagination support. |
| `CreateDocument` | `POST /v1/{parent=...}/documents/{collection_id}` | ✅ Implemented | Creates a document with explicit or server-generated ID. |
| `UpdateDocument` | `PATCH /v1/{name=projects/*/databases/*/documents/*/*}` | ✅ Implemented | Updates a document; supports update masks and preconditions. |
| `DeleteDocument` | `DELETE /v1/{name=projects/*/databases/*/documents/*/*}` | ✅ Implemented | Deletes a single document by path. |
| `BatchGetDocuments` | `POST /v1/{database=...}/documents:batchGet` | ✅ Implemented | Streams multiple documents by resource path. |
| `Commit` | `POST /v1/{database=...}/documents:commit` | ✅ Implemented | Atomically applies document writes, transforms, and deletes. |
| `RunQuery` | `POST /v1/{parent=...}/documents:runQuery` | ✅ Implemented | Executes structured queries and streams matching documents. |
| `RunAggregationQuery` | `POST /v1/{parent=...}/documents:runAggregationQuery` | ✅ Implemented | Computes aggregate values (`COUNT`, `SUM`, `AVG`) across query results. |
| `Listen` | Stream `google.firestore.v1.Firestore/Listen` | ✅ Implemented | Subscribes to real-time document and query watch updates. |
| `Write` | Stream `google.firestore.v1.Firestore/Write` | ✅ Implemented | Streams batch write operations (used by WebChannel clients). |
| `BeginTransaction` | `POST /v1/{database=...}/documents:beginTransaction` | ✅ Implemented | Initiates an interactive transaction session. |
| `Rollback` | `POST /v1/{database=...}/documents:rollback` | ✅ Implemented | Aborts an active transaction session. |
| `PartitionQuery` | `POST /v1/{parent=...}/documents:partitionQuery` | ⚠️ Planned | Query partitioning for parallel map-reduce style processing. |

---

## 🔍 Structured Query Capabilities

Embyr translates Firestore `StructuredQuery` definitions directly into optimized SQL queries.

### Field Path Operations & Filter Operators

| Operator | Symbol | SQL Translation | Supported |
| :--- | :--- | :--- | :---: |
| `EQUAL` | `==` | `JSON_EXTRACT(data, path) = val` | ✅ |
| `NOT_EQUAL` | `!=` | `JSON_EXTRACT(data, path) != val` | ✅ |
| `LESS_THAN` | `<` | `JSON_EXTRACT(data, path) < val` | ✅ |
| `LESS_THAN_OR_EQUAL` | `<=` | `JSON_EXTRACT(data, path) <= val` | ✅ |
| `GREATER_THAN` | `>` | `JSON_EXTRACT(data, path) > val` | ✅ |
| `GREATER_THAN_OR_EQUAL` | `>=` | `JSON_EXTRACT(data, path) >= val` | ✅ |
| `IN` | `in` | `JSON_EXTRACT(data, path) IN (...)` | ✅ |
| `NOT_IN` | `not-in` | `JSON_EXTRACT(data, path) NOT IN (...)` | ✅ |
| `ARRAY_CONTAINS` | `array-contains` | JSON array element match | ✅ |
| `ARRAY_CONTAINS_ANY` | `array-contains-any` | JSON array intersection match | ✅ |

### Query Features

* **Dot Notation**: Full support for nested field path traversals (e.g. `user.profile.age`).
* **Composite Filters**: Supports nested `AND` and `OR` filter groups.
* **Ordering & Pagination**: Supports `ORDER BY`, `LIMIT`, and `OFFSET`.
* **Collection Group Queries**: Queries matching collection IDs across arbitrary document hierarchy paths.
* **Aggregation Functions**: Native execution of `count(*)`, `sum(field)`, and `avg(field)`.

---

## ⚠️ Key Differences & Known Limitations

For complete technical specifications on exact wire protocol divergences, consult [contract.md](file:///Users/petervyboch/Projects/embyr/contract.md).

1. **Transaction Isolation**: Embyr implements transaction isolation backed by PostgreSQL/SQLite serializable transactions rather than Google's internal Spanner TrueTime mechanism.
2. **Security Rules Engine**: Embyr relies on server-side authentication interceptors (`auth.mode`) rather than Firestore Security Rules (`.rules` files).
3. **Index Management**: Automatic single-field indexing is handled via SQL database indices rather than Google Firestore composite index configurations (`firestore.indexes.json`).
