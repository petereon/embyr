# Embyr

**Embyr** is an open-source, high-performance [Google Cloud Firestore](https://cloud.google.com/firestore) compatibility layer written in Go. It enables applications built for Firestore to run seamlessly against **PostgreSQL** or **SQLite** as the storage backend without modifying client code or database drivers.

---

## ⚡ Key Features

* **Complete Firestore Protocol Support**: Speaks native gRPC, gRPC-Web, WebChannel (browser long-polling), and REST (grpc-gateway).
* **Multiple Database Backends**:
  * **PostgreSQL**: Built for production with connection pooling (`pgx`) and real-time updates via PostgreSQL `LISTEN/NOTIFY`.
  * **SQLite**: Embedded, zero-dependency storage ideal for local development, embedded systems, and lightweight deployments.
* **Real-time Subscriptions (`Listen`)**: Supports live document and query subscriptions (`watch` streams) across all transports.
* **ACID Transactions**: Full support for interactive transactions, batch reads/writes, and optimistic concurrency control (`OCC`).
* **Flexible Authentication**: Pluggable authentication supporting `none` (dev mode), static API keys (`key`), Google OAuth2 Access Token validation (`google`), and mutual TLS (`mtls`).
* **Schema-less Document Storage**: Native document representation with JSON field indexing and full proto3 JSON translation.

---

## 🏗️ Architecture at a Glance

Embyr acts as a drop-in replacement for the Firestore emulator or production Firestore service:

```mermaid
graph TD
    Client[Firestore Client / SDK / App] -->|gRPC :8080| Server[Embyr Server]
    Client -->|REST / gRPC-Web / WebChannel :8081| Server
    Server --> Auth[Auth Interceptors]
    Server --> Codec[Codec - Proto / SQL Translator]
    Server --> Listen[Listener Registry & Watch Stream]
    Server --> Storage[Storage Adapter]
    Storage --> Postgres[(PostgreSQL)]
    Storage --> SQLite[(SQLite)]
```

For a deep dive into the internal design, see the [Architecture Guide](docs/ARCHITECTURE.md).

---

## 🚀 Quickstart

### Prerequisites

* Go 1.25+ (if running from source)
* Docker & Docker Compose (for containerized setup)

### Running with Docker Compose (PostgreSQL Backend)

The fastest way to get started is using Docker Compose:

```bash
docker-compose up --build
```

This starts:
* PostgreSQL 16 on internal network
* Embyr on ports `17080` (gRPC) and `17081` (REST / gRPC-Web / WebChannel)

### Running Standalone (SQLite Backend)

Build and run Embyr locally with an embedded SQLite database:

```bash
# Build the binary
go build -o embyr ./cmd/embyr

# Start with default SQLite configuration (creates embyr.db)
./embyr
```

By default, Embyr listens on:
* **gRPC**: `127.0.0.1:8080`
* **REST / gRPC-Web / WebChannel**: `127.0.0.1:8081`

---

## ⚙️ Configuration Overview

Embyr can be configured using a YAML configuration file or environment variables prefixed with `EMBYR_`.

```yaml
server:
  grpc_port: 8080
  rest_port: 8081
  allowed_origins: []

auth:
  mode: "none" # Options: none, key, google, mtls

backend:
  type: "sqlite" # Options: sqlite, postgres
  sqlite:
    path: "embyr.db"
  postgres:
    dsn: "postgres://embyr:embyr@localhost:5432/embyr?sslmode=disable"
    max_conns: 25

transactions:
  ttl: "60s"
  sweep_interval: "30s"

log:
  level: "info"
  format: "json"
```

For full details on environment variable bindings and security settings, see the [Configuration Guide](docs/CONFIGURATION.md).

---

## 📚 Documentation Index

| Guide | Description |
| :--- | :--- |
| **[Architecture](docs/ARCHITECTURE.md)** | Technical breakdown of internals, storage drivers, request lifecycles, and notification loops. |
| **[Configuration](docs/CONFIGURATION.md)** | Complete reference for YAML settings, `EMBYR_*` environment variables, and authentication setup. |
| **[API Compatibility](docs/COMPATIBILITY.md)** | Firestore v1 RPC support matrix, query feature support, and protocol details. |
| **[Development & Testing](docs/DEVELOPMENT.md)** | Developer quickstart, running unit/integration/browser tests, and database migrations. |
| **[Wire Contract Spec](contract.md)** | Authoritative low-level wire protocol specification for gRPC, REST, and WebChannel. |

---

## 🧪 Testing

Embyr includes comprehensive test suites across Go, Node.js integration tests, and Playwright browser E2E tests:

```bash
# Run Go unit and package tests
go test ./... -v

# Run Node.js integration tests
cd test/integration && bun install && bun test

# Run Browser E2E tests
cd test/browser && bun install && npx playwright test
```

For step-by-step instructions on running tests and the React demo app, see the [Development Guide](docs/DEVELOPMENT.md).

---

## 📄 License

Embyr is licensed under the MIT License.
