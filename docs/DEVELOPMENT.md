# Embyr — Developer & Testing Guide

This guide outlines setup instructions, build procedures, testing methodologies, and workflow guidelines for developers working on **Embyr**.

---

## 🛠️ Prerequisites & Development Tools

Ensure you have the following installed before contributing to Embyr:

* **Go**: Version 1.25+
* **Bun**: Recommended package manager for JS/Node tooling (or Node.js 18+ with `npm`/`pnpm`)
* **Buf**: Protobuf generation tool ([buf.build](https://buf.build/))
* **Docker & Docker Compose**: Required for running PostgreSQL integration testing containers

---

## 🏗️ Building Embyr

### Local Build

Build the `embyr` binary using `go` or `make`:

```bash
# Using Makefile
make build

# Using Go CLI directly
CGO_ENABLED=0 go build -o embyr ./cmd/embyr
```

### Running Locally

```bash
# Run with default SQLite database (embyr.db)
./embyr

# Run with custom config file
./embyr --config ./config.yaml
```

---

## 🧪 Testing Suites

Embyr enforces quality control through three complementary test suites:

### 1. Go Package Unit & Integration Tests

Go tests cover core server logic, store implementations (SQLite & Postgres), codec translations, and auth interceptors.

```bash
# Run all Go tests
go test ./... -v -count=1

# Run tests for a specific package (e.g. server)
go test ./internal/server -v
```

### 2. Node.js Integration Tests (Vitest)

Integration tests located in `test/integration/` verify client compatibility using official Firebase/Firestore JavaScript SDKs against a running Embyr server.

```bash
cd test/integration

# Install dependencies with Bun
bun install

# Run integration tests with Vitest
bun test
# OR using Vitest directly
npx vitest
```

### 3. Browser E2E Tests (Playwright)

End-to-end browser tests in `test/browser/` test real-time WebChannel long-polling and client synchronization in headless browser environments.

```bash
cd test/browser

# Install dependencies
bun install

# Run Playwright browser tests
npx playwright test
```

### 4. Demo React Application

A demonstration React application (built with Vite) is available in `demo-react/` to interactively test Embyr features:

```bash
cd demo-react

# Install dependencies
bun install

# Start Vite development server
bun run dev
```

---

## 📜 Protobuf Generation Workflow

Embyr uses **Buf** to manage Google Firestore Protobuf schemas (`buf.yaml`, `buf.gen.yaml`) and generate Go structures under `gen/go/google/firestore/v1/`.

To regenerate Protobuf Go code after schema changes:

```bash
# Using Makefile
make proto

# Using Buf CLI directly
buf generate
```

---

## 🗄️ Database Migrations

Embyr uses `golang-migrate` for versioned database schema updates:

* **PostgreSQL Migrations**: Located in [migrations/postgres](file:///Users/petervyboch/Projects/embyr/migrations/postgres) (`.up.sql` / `.down.sql`).
* **SQLite Migrations**: Located in [migrations/sqlite](file:///Users/petervyboch/Projects/embyr/migrations/sqlite) (`.up.sql` / `.down.sql`).

Migrations run automatically upon server initialization (`adapter.Migrate(ctx)`).

To add a new database migration:
1. Create sequentially numbered `.up.sql` and `.down.sql` files in both `migrations/postgres/` and `migrations/sqlite/`.
2. Ensure triggers or JSON functions adhere to engine-specific SQL syntax.
