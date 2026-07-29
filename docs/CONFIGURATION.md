# Embyr Configuration Reference

Embyr supports configuration via YAML files or environment variables. Environment variables take precedence over configuration file values.

---

## 🛠️ Loading Configuration

To specify a YAML configuration file, use the `--config` CLI flag:

```bash
./embyr --config /path/to/config.yaml
```

If `--config` is omitted, Embyr loads standard default values and evaluates environment variables.

---

## 📋 Complete Configuration Reference

### YAML Schema & Default Values

```yaml
server:
  grpc_port: 8080
  rest_port: 8081
  tls:
    cert: ""
    key: ""
  allowed_origins: [] # Empty list allows all origins (CORS)

auth:
  mode: "none" # Options: none, key, google, mtls
  key: ""
  google_project_id: ""
  mtls_ca: ""

backend:
  type: "sqlite" # Options: sqlite, postgres
  sqlite:
    path: "embyr.db"
  postgres:
    dsn: ""
    max_conns: 25

transactions:
  ttl: "60s"
  sweep_interval: "30s"

log:
  level: "info" # Options: debug, info, warn, error
  format: "json" # Options: json, console
```

---

## 🌐 Environment Variables

Environment variables match configuration file keys using the `EMBYR_` prefix and uppercase snake_case syntax (dots replace underscores).

| Environment Variable | Equivalent Config Key | Default Value | Description |
| :--- | :--- | :--- | :--- |
| `EMBYR_SERVER_GRPC_PORT` | `server.grpc_port` | `8080` | Port for standard gRPC (HTTP/2) traffic. |
| `EMBYR_SERVER_REST_PORT` | `server.rest_port` | `8081` | Port for REST, gRPC-Web, WebChannel, and health checks. |
| `EMBYR_SERVER_TLS_CERT` | `server.tls.cert` | `""` | Path to TLS certificate file. |
| `EMBYR_SERVER_TLS_KEY` | `server.tls.key` | `""` | Path to TLS private key file. |
| `EMBYR_AUTH_MODE` | `auth.mode` | `"none"` | Authentication mode (`none`, `key`, `google`, `mtls`). |
| `EMBYR_AUTH_KEY` | `auth.key` | `""` | Static API key (required when `auth.mode = "key"`). |
| `EMBYR_AUTH_GOOGLE_PROJECT_ID` | `auth.google_project_id` | `""` | Google Cloud Project ID (required when `auth.mode = "google"`). |
| `EMBYR_AUTH_MTLS_CA` | `auth.mtls_ca` | `""` | Path to CA certificate bundle (required when `auth.mode = "mtls"`). |
| `EMBYR_BACKEND_TYPE` | `backend.type` | `"sqlite"` | Database engine (`sqlite` or `postgres`). |
| `EMBYR_BACKEND_SQLITE_PATH` | `backend.sqlite.path` | `"embyr.db"` | SQLite database file path. |
| `EMBYR_BACKEND_POSTGRES_DSN` | `backend.postgres.dsn` | `""` | PostgreSQL connection string / DSN. |
| `EMBYR_BACKEND_POSTGRES_MAX_CONNS` | `backend.postgres.max_conns` | `25` | Maximum database connection pool size. |
| `EMBYR_TRANSACTIONS_TTL` | `transactions.ttl` | `"60s"` | Inactive transaction timeout duration. |
| `EMBYR_TRANSACTIONS_SWEEP_INTERVAL` | `transactions.sweep_interval` | `"30s"` | Frequency of background transaction garbage collection. |
| `EMBYR_LOG_LEVEL` | `log.level` | `"info"` | Logging severity filter. |
| `EMBYR_LOG_FORMAT` | `log.format` | `"json"` | Log format (`json` or `console`). |

---

## 🗄️ Database Backend Setup

### PostgreSQL Setup

To use PostgreSQL, set `backend.type` to `postgres` and provide a PostgreSQL connection DSN:

```bash
export EMBYR_BACKEND_TYPE="postgres"
export EMBYR_BACKEND_POSTGRES_DSN="postgres://user:password@localhost:5432/embyr?sslmode=disable"
export EMBYR_BACKEND_POSTGRES_MAX_CONNS="50"
```

Database migrations are automatically applied on startup from the `migrations/postgres` directory.

### SQLite Setup

SQLite is the default storage engine and requires no external services:

```bash
export EMBYR_BACKEND_TYPE="sqlite"
export EMBYR_BACKEND_SQLITE_PATH="./data/embyr.db"
```

Database migrations are automatically applied on startup from `migrations/sqlite`.

---

## 🔑 Authentication Modes

Embyr supports 4 authentication modes:

### 1. `none` (Development Default)
Disables authentication checks. Useful for local testing and emulator compatibility.

### 2. `key` (Static Bearer Key)
Requires client requests to pass a static token via the `authorization: Bearer <key>` header or `x-api-key` header:

```yaml
auth:
  mode: "key"
  key: "secret-embyr-api-token"
```

### 3. `google` (OAuth2 Access Token Verification)
Validates Google OAuth2 access tokens against Google's OAuth2 API (`tokeninfo`). Enforces matching project audience:

```yaml
auth:
  mode: "google"
  google_project_id: "my-gcp-project-id"
```

### 4. `mtls` (Mutual TLS)
Enforces client certificate validation using a trusted CA certificate:

```yaml
auth:
  mode: "mtls"
  mtls_ca: "/etc/embyr/certs/ca.crt"
server:
  tls:
    cert: "/etc/embyr/certs/server.crt"
    key: "/etc/embyr/certs/server.key"
```
