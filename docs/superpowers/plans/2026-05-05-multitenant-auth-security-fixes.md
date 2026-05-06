# Multi-Tenant Auth Security Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close three independently exploitable authentication bypass vulnerabilities in multi-tenant mode so that no request — gRPC, REST, or WebChannel — can reach a tenant's data without a valid token.

**Architecture:** All three bypasses share a root cause: tenant resolution (`resolveTenant`) successfully looks up the tenant's auth config but never validates the caller's token against it. The fixes are: (1) strip the `/v1/` REST URL prefix so `ParseTenantKey` parses real Firestore URLs, (2) call `auth.ValidateForTenant` inside `resolveTenant` after every successful lookup, and (3) thread an authenticated-context resolver through WebChannel handlers so session goroutines inherit per-tenant auth. A `FactoryLike` interface makes each layer testable in isolation.

**Tech Stack:** Go, `google.golang.org/grpc/metadata`, `google.golang.org/grpc/status`, `github.com/petereon/embyr/internal/auth`, `net/http/httptest`, `github.com/stretchr/testify`.

---

## File Map

| File | Change |
|------|--------|
| `internal/tenancy/middleware.go` | Fix `ParseTenantKey`; add fail-closed + `ValidateForTenant` call to `resolveTenant`; inject HTTP `Authorization` header as gRPC metadata in `HTTPMiddleware`; add `NewResolveFn`; change all `*AdapterFactory` params to `FactoryLike` |
| `internal/tenancy/middleware_test.go` | Add test cases for `/v1/` URL forms and auth enforcement via `HTTPMiddleware` |
| `internal/tenancy/factory.go` | Add `FactoryLike` interface; add `rawAuthConfig []byte` to `entry`; update `Get` to return `(store.StorageAdapter, *auth.Config, []byte, error)` |
| `internal/webchannel/handler.go` | Add `TenantResolveFn` type; add `resolveFn` field to `Handler` and `WriteHandler`; apply in `handleForward` for new sessions |
| `internal/server/server.go` | Change `NewMultiTenant` factory param to `tenancy.FactoryLike`; wire `tenancy.NewResolveFn` into webchannel handlers |
| `internal/server/multitenant_auth_test.go` | New: server-level auth matrix tests using fake `FactoryLike` and sqlite adapters |

---

## Task 1: Fix ParseTenantKey for real Firestore REST URL paths

**Files:**
- Modify: `internal/tenancy/middleware_test.go`
- Modify: `internal/tenancy/middleware.go`

Real Firestore REST URLs have a `/v1/` prefix (`/v1/projects/p/databases/d/documents/...`). `ParseTenantKey` splits on `/` and checks `parts[0] == "projects"`. For URLs with `/v1/`, the leading `/` makes `parts[0]` empty, so every HTTP REST request silently bypasses tenant resolution.

- [ ] **Step 1: Write failing tests**

Add these cases to the existing `TestParseTenantKey` table in `internal/tenancy/middleware_test.go`. Append to the `cases` slice (before the closing `}`):

```go
// Real Firestore REST URL forms — these all fail before the fix.
{"/v1/projects/acme/databases/prod/documents/users/alice", "acme", "prod", true},
{"/v1/projects/acme/databases/prod", "acme", "prod", true},
{"/v1/projects/acme/databases/prod/documents:batchGet", "acme", "prod", true},
{"v1/projects/acme/databases/prod", "acme", "prod", true},
// Empty segments still rejected.
{"/v1/projects//databases/prod", "", "", false},
{"/v1/projects/acme/databases/", "", "", false},
```

- [ ] **Step 2: Run test and verify failure**

```bash
cd /Users/petervyboch/Projects/embyr/.worktrees/multi-tenancy
go test ./internal/tenancy/... -run TestParseTenantKey -v
```

Expected: FAIL on the new `/v1/` cases — `ok` is `false` instead of `true`.

- [ ] **Step 3: Fix ParseTenantKey**

In `internal/tenancy/middleware.go`, replace the body of `ParseTenantKey` with:

```go
func ParseTenantKey(path string) (projectID, databaseID string, ok bool) {
	// Strip the REST API version prefix used by Firestore REST and gRPC-gateway URLs.
	// Proto message fields (name, parent, database) never carry this prefix, so the
	// strip is a no-op for gRPC paths.
	path = strings.TrimPrefix(path, "/v1/")
	path = strings.TrimPrefix(path, "v1/")

	parts := strings.SplitN(path, "/", 5)
	if len(parts) < 4 {
		return "", "", false
	}
	if parts[0] != "projects" || parts[2] != "databases" {
		return "", "", false
	}
	if parts[1] == "" || parts[3] == "" {
		return "", "", false
	}
	return parts[1], parts[3], true
}
```

- [ ] **Step 4: Run tests and verify pass**

```bash
go test ./internal/tenancy/... -run TestParseTenantKey -v
```

Expected: PASS for all cases.

- [ ] **Step 5: Run full test suite**

```bash
go test ./... 2>&1 | tail -20
```

Expected: no new failures.

- [ ] **Step 6: Commit**

```bash
git add internal/tenancy/middleware.go internal/tenancy/middleware_test.go
git commit -m "fix: ParseTenantKey strips /v1/ prefix so HTTP REST paths resolve to tenants"
```

---

## Task 2: FactoryLike interface + raw auth config in AdapterFactory.Get

**Files:**
- Modify: `internal/tenancy/factory.go`
- Modify: `internal/tenancy/middleware.go`

`resolveTenant` needs the raw JSON auth config (e.g. `{"key":"abc"}`) to pass to `auth.ValidateForTenant`. Currently `factory.Get` returns `*auth.Config` whose `RawConfig()` is always nil — `auth.New` never populates the unexported `rawConfig` field. The fix: store the raw bytes from `tenant.AuthConfig` in the factory cache entry and return them as a fourth value from `Get`. A `FactoryLike` interface makes the middleware testable.

- [ ] **Step 1: Add FactoryLike interface and update entry + Get in factory.go**

In `internal/tenancy/factory.go`:

a) Add `"encoding/json"` is already imported. Add the `FactoryLike` interface right before the `ErrTenantSuspended` var:

```go
// FactoryLike is the subset of AdapterFactory used by middleware.
// Implemented by *AdapterFactory; can be replaced with a test double.
type FactoryLike interface {
	Get(ctx context.Context, projectID, databaseID string) (store.StorageAdapter, *auth.Config, []byte, error)
}
```

b) Update `entry` struct to carry the raw auth config:

```go
type entry struct {
	adapter       store.StorageAdapter
	authConfig    *auth.Config
	rawAuthConfig []byte
}
```

c) Update the `Get` method signature and every return site:

```go
// Get returns (adapter, authConfig, rawAuthConfig, nil) for (projectID, databaseID).
// rawAuthConfig is the JSON bytes from the registry (e.g. {"key":"…"}) suitable for
// passing to auth.ValidateForTenant.
// On cache miss: resolves credential, opens Postgres pool, pings, caches.
func (f *AdapterFactory) Get(ctx context.Context, projectID, databaseID string) (store.StorageAdapter, *auth.Config, []byte, error) {
	key := factoryCacheKey(projectID, databaseID)

	f.mu.Lock()
	if e, ok := f.cache.Get(key); ok {
		f.mu.Unlock()
		return e.adapter, e.authConfig, e.rawAuthConfig, nil
	}
	f.mu.Unlock()

	tenant, err := f.reg.Get(ctx, projectID, databaseID)
	if err != nil {
		return nil, nil, nil, err
	}
	if tenant.Status == registry.TenantStatusSuspended {
		return nil, nil, nil, ErrTenantSuspended
	}

	resolver, err := NewResolver(tenant)
	if err != nil {
		return nil, nil, nil, err
	}
	dsn, err := resolver.Resolve(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("tenancy: resolve credential for %s/%s: %w", projectID, databaseID, err)
	}

	dsn, err = appendSearchPath(dsn, tenant.SchemaName)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("tenancy: append search_path: %w", err)
	}

	adapter, err := postgres.New(dsn, "", 5)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("tenancy: open postgres for %s/%s: %w", projectID, databaseID, err)
	}
	if err := adapter.Ping(ctx); err != nil {
		adapter.Close() //nolint:errcheck
		return nil, nil, nil, fmt.Errorf("tenancy: ping %s/%s: %w", projectID, databaseID, err)
	}

	authCfg, err := buildAuthConfig(tenant)
	if err != nil {
		adapter.Close() //nolint:errcheck
		return nil, nil, nil, fmt.Errorf("tenancy: build auth config for %s/%s: %w", projectID, databaseID, err)
	}

	rawAuthConfig := []byte(tenant.AuthConfig)

	e := &entry{adapter: adapter, authConfig: authCfg, rawAuthConfig: rawAuthConfig}
	f.mu.Lock()
	if existing, ok := f.cache.Get(key); ok {
		f.mu.Unlock()
		adapter.Close() //nolint:errcheck
		return existing.adapter, existing.authConfig, existing.rawAuthConfig, nil
	}
	f.cache.Add(key, e)
	f.mu.Unlock()

	f.log.Info("tenancy: adapter opened", zap.String("project", projectID), zap.String("database", databaseID))
	return adapter, authCfg, rawAuthConfig, nil
}
```

d) Add `var _ FactoryLike = (*AdapterFactory)(nil)` after the existing `var _ io.Closer = (*AdapterFactory)(nil)` line to enforce the interface at compile time.

- [ ] **Step 2: Update middleware.go to use FactoryLike and 4-value Get**

In `internal/tenancy/middleware.go`:

a) Add import `"github.com/petereon/embyr/internal/auth"` to the import block.

b) Change `func UnaryInterceptor(factory *AdapterFactory, log *zap.Logger)` to:
```go
func UnaryInterceptor(factory FactoryLike, log *zap.Logger) grpc.UnaryServerInterceptor {
```

c) Change `func StreamInterceptor(factory *AdapterFactory, log *zap.Logger)` to:
```go
func StreamInterceptor(factory FactoryLike, log *zap.Logger) grpc.StreamServerInterceptor {
```

d) Change `peekStream.factory` field type:
```go
type peekStream struct {
	grpc.ServerStream
	factory  FactoryLike
	log      *zap.Logger
	resolved bool
}
```

e) Change `func HTTPMiddleware(factory *AdapterFactory, log *zap.Logger, next http.Handler)` to:
```go
func HTTPMiddleware(factory FactoryLike, log *zap.Logger, next http.Handler) http.Handler {
```

f) Update `resolveTenant` signature and body to use the 4-value return. **Do not add ValidateForTenant yet** — that is Task 3. The update for now is just to receive and store rawAuthCfg:

```go
func resolveTenant(ctx context.Context, factory FactoryLike, log *zap.Logger, path string) (context.Context, error) {
	projectID, databaseID, ok := ParseTenantKey(path)
	if !ok {
		return ctx, nil // Task 3 will make this fail-closed
	}

	adapter, authCfg, rawAuthCfg, err := factory.Get(ctx, projectID, databaseID)
	if err != nil {
		if errors.Is(err, ErrTenantSuspended) || errors.Is(err, registry.ErrTenantNotFound) {
			return ctx, status.Error(codes.Unauthenticated, "unauthenticated")
		}
		log.Warn("tenancy: factory error", zap.Error(err),
			zap.String("project", projectID), zap.String("database", databaseID))
		return ctx, status.Error(codes.Internal, "internal error")
	}

	ctx = WithAdapter(ctx, adapter)
	ctx = WithAuthInfo(ctx, AuthInfo{
		Mode:   authCfg.Mode(),
		Config: rawAuthCfg, // now correctly populated from registry tenant.AuthConfig
	})
	return ctx, nil
}
```

- [ ] **Step 3: Run tests**

```bash
go test ./... 2>&1 | tail -20
```

Expected: all existing tests pass. The only change is the factory signature; behaviour is unchanged.

- [ ] **Step 4: Commit**

```bash
git add internal/tenancy/factory.go internal/tenancy/middleware.go
git commit -m "refactor: FactoryLike interface + expose raw auth config from AdapterFactory.Get"
```

---

## Task 3: Enforce auth in resolveTenant + fail-closed + HTTP Bearer injection

**Files:**
- Modify: `internal/tenancy/middleware.go`
- Modify: `internal/tenancy/middleware_test.go`

This task wires in the three enforcement changes: fail-closed when the path doesn't parse (prevents unknown-path bypass), HTTP `Authorization` header injection as gRPC metadata (so `auth.ValidateForTenant` → `bearerToken` works for both gRPC and HTTP contexts), and the actual `auth.ValidateForTenant` call inside `resolveTenant`.

- [ ] **Step 1: Write failing tests**

Add these tests to `internal/tenancy/middleware_test.go`. They test `HTTPMiddleware` because `resolveTenant` is unexported. Add a `stubFactory` type at the top of the file (inside `package tenancy_test`) and then the four test functions:

```go
package tenancy_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/petereon/embyr/internal/auth"
	"github.com/petereon/embyr/internal/config"
	"github.com/petereon/embyr/internal/store"
	"github.com/petereon/embyr/internal/tenancy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// stubFactory implements tenancy.FactoryLike for middleware tests.
type stubFactory struct {
	authConfig    *auth.Config
	rawAuthConfig []byte
	// errToReturn is returned by Get when set; simulates lookup failures.
	errToReturn error
}

func (f *stubFactory) Get(_ context.Context, _, _ string) (store.StorageAdapter, *auth.Config, []byte, error) {
	if f.errToReturn != nil {
		return nil, nil, nil, f.errToReturn
	}
	return nil, f.authConfig, f.rawAuthConfig, nil
}

// newKeyStub returns a stubFactory pre-configured for "key" auth mode.
func newKeyStub(t *testing.T, key string) *stubFactory {
	t.Helper()
	rawCfg, err := json.Marshal(map[string]string{"key": key})
	require.NoError(t, err)
	authCfg, err := auth.New(
		&config.Config{Auth: config.AuthConfig{Mode: "key", Key: key}},
		zap.NewNop(),
	)
	require.NoError(t, err)
	return &stubFactory{authConfig: authCfg, rawAuthConfig: rawCfg}
}

func okHandler(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }

func TestHTTPMiddleware_RejectsUnparsablePath(t *testing.T) {
	// Path has no tenant key — middleware must fail-closed with 401.
	factory := newKeyStub(t, "secret")
	handler := tenancy.HTTPMiddleware(factory, zap.NewNop(), http.HandlerFunc(okHandler))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestHTTPMiddleware_RejectsMissingAuth(t *testing.T) {
	// Valid tenant path but no Authorization header.
	factory := newKeyStub(t, "secret")
	handler := tenancy.HTTPMiddleware(factory, zap.NewNop(), http.HandlerFunc(okHandler))

	req := httptest.NewRequest(http.MethodGet,
		"/v1/projects/proj/databases/db/documents/users/alice", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestHTTPMiddleware_RejectsWrongKey(t *testing.T) {
	// Valid tenant path with wrong API key.
	factory := newKeyStub(t, "correct-key")
	handler := tenancy.HTTPMiddleware(factory, zap.NewNop(), http.HandlerFunc(okHandler))

	req := httptest.NewRequest(http.MethodGet,
		"/v1/projects/proj/databases/db/documents/users/alice", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestHTTPMiddleware_AcceptsCorrectKey(t *testing.T) {
	// Valid tenant path with correct key — request reaches next handler.
	factory := newKeyStub(t, "correct-key")
	handler := tenancy.HTTPMiddleware(factory, zap.NewNop(), http.HandlerFunc(okHandler))

	req := httptest.NewRequest(http.MethodGet,
		"/v1/projects/proj/databases/db/documents/users/alice", nil)
	req.Header.Set("Authorization", "Bearer correct-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHTTPMiddleware_RejectsUnknownTenant(t *testing.T) {
	// Factory returns ErrTenantNotFound — must map to 401 (tenant enumeration prevention).
	import_registry_err_factory := &stubFactory{errToReturn: registry.ErrTenantNotFound}
	handler := tenancy.HTTPMiddleware(import_registry_err_factory, zap.NewNop(), http.HandlerFunc(okHandler))

	req := httptest.NewRequest(http.MethodGet,
		"/v1/projects/ghost/databases/db/documents/users/alice", nil)
	req.Header.Set("Authorization", "Bearer any-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}
```

> **Note:** `TestHTTPMiddleware_RejectsUnknownTenant` above has an intentional placeholder for a proper import — fix it: add `"github.com/petereon/embyr/internal/registry"` to the imports and use `&stubFactory{errToReturn: registry.ErrTenantNotFound}` directly.

Final imports block for `middleware_test.go`:
```go
import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/petereon/embyr/internal/auth"
	"github.com/petereon/embyr/internal/config"
	"github.com/petereon/embyr/internal/registry"
	"github.com/petereon/embyr/internal/store"
	"github.com/petereon/embyr/internal/tenancy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)
```

- [ ] **Step 2: Run tests and confirm they fail**

```bash
go test ./internal/tenancy/... -run "TestHTTPMiddleware" -v
```

Expected: `TestHTTPMiddleware_RejectsUnparsablePath` **passes** (path returns nil, next not called... actually wait — currently `!ok` returns `ctx, nil` so the middleware calls `next` and next returns 200, but we expect 401). All four tests should FAIL — they currently expect 401 but the middleware passes everything through.

- [ ] **Step 3: Implement the three enforcement changes in middleware.go**

Replace the current `HTTPMiddleware` and `resolveTenant` with the full enforcing versions. Also add `NewResolveFn`. Add `"google.golang.org/grpc/metadata"` to the import block.

**HTTPMiddleware** — inject Authorization header as gRPC metadata before resolving tenant:

```go
// HTTPMiddleware wraps an http.Handler, extracting the tenant key from the
// URL path, injecting the adapter and validating the caller's token.
func HTTPMiddleware(factory FactoryLike, log *zap.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		// Inject the HTTP Authorization header as gRPC incoming metadata so
		// auth.ValidateForTenant → bearerToken(ctx) works for HTTP contexts too.
		if h := r.Header.Get("Authorization"); h != "" {
			md := metadata.New(map[string]string{"authorization": h})
			ctx = metadata.NewIncomingContext(ctx, md)
		}
		ctx, err := resolveTenant(ctx, factory, log, r.URL.Path)
		if err != nil {
			st, _ := status.FromError(err)
			http.Error(w, st.Message(), grpcCodeToHTTPStatus(st.Code()))
			return
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
```

**resolveTenant** — fail-closed and call ValidateForTenant:

```go
func resolveTenant(ctx context.Context, factory FactoryLike, log *zap.Logger, path string) (context.Context, error) {
	projectID, databaseID, ok := ParseTenantKey(path)
	if !ok {
		// Fail-closed: any request whose path cannot be parsed to a tenant key
		// is rejected. Health-check routes (/healthz, /readyz) are registered
		// on more-specific mux patterns and never reach this middleware.
		return ctx, status.Error(codes.Unauthenticated, "unauthenticated")
	}

	adapter, authCfg, rawAuthCfg, err := factory.Get(ctx, projectID, databaseID)
	if err != nil {
		if errors.Is(err, ErrTenantSuspended) || errors.Is(err, registry.ErrTenantNotFound) {
			return ctx, status.Error(codes.Unauthenticated, "unauthenticated")
		}
		log.Warn("tenancy: factory error", zap.Error(err),
			zap.String("project", projectID), zap.String("database", databaseID))
		return ctx, status.Error(codes.Internal, "internal error")
	}

	ctx = WithAdapter(ctx, adapter)
	ctx = WithAuthInfo(ctx, AuthInfo{
		Mode:   authCfg.Mode(),
		Config: rawAuthCfg,
	})

	// Validate the caller's token against this tenant's stored auth config.
	// For gRPC requests, bearerToken reads from gRPC incoming metadata.
	// For HTTP requests, HTTPMiddleware injected the Authorization header above.
	if err := auth.ValidateForTenant(ctx, authCfg.Mode(), rawAuthCfg); err != nil {
		return ctx, err
	}
	return ctx, nil
}
```

**NewResolveFn** — add at the bottom of middleware.go (used by webchannel in Task 4):

```go
// NewResolveFn returns a function that resolves and authenticates a tenant
// from a Firestore resource path. Intended for use in non-middleware contexts
// (e.g., WebChannel handlers) that cannot use gRPC interceptors.
// The returned function expects gRPC incoming metadata on ctx (caller must
// inject the HTTP Authorization header before calling).
func NewResolveFn(factory FactoryLike, log *zap.Logger) func(ctx context.Context, path string) (context.Context, error) {
	return func(ctx context.Context, path string) (context.Context, error) {
		return resolveTenant(ctx, factory, log, path)
	}
}
```

- [ ] **Step 4: Run the new tests**

```bash
go test ./internal/tenancy/... -run "TestHTTPMiddleware" -v
```

Expected: all five tests PASS.

- [ ] **Step 5: Run full test suite**

```bash
go test ./... 2>&1 | tail -30
```

Expected: no regressions. (The peekStream / gRPC interceptors also use `resolveTenant`, so gRPC paths are fixed too.)

- [ ] **Step 6: Commit**

```bash
git add internal/tenancy/middleware.go internal/tenancy/middleware_test.go
git commit -m "fix: enforce per-tenant auth in resolveTenant — fail-closed, ValidateForTenant, HTTP Bearer injection"
```

---

## Task 4: WebChannel session authentication

**Files:**
- Modify: `internal/webchannel/handler.go`
- Modify: `internal/server/server.go`

WebChannel Listen and Write handlers create session goroutines with `context.Background()`. In multi-tenant mode this means no adapter and no auth validation — the bridge context bypasses everything. The fix: accept a `TenantResolveFn` at construction, call it at new-session creation time with the database path from the first parsed message, and use the resulting authenticated context for the bridge.

- [ ] **Step 1: Add TenantResolveFn to webchannel/handler.go**

At the top of `internal/webchannel/handler.go`, add the type definition right before `listenBridge`:

```go
// TenantResolveFn resolves and authenticates a tenant from a Firestore resource path.
// Returning a non-nil error aborts session establishment with the appropriate HTTP status.
// Set to nil in single-tenant mode (no-op).
type TenantResolveFn func(ctx context.Context, path string) (context.Context, error)
```

Add imports `"google.golang.org/grpc/codes"` and `"google.golang.org/grpc/status"` (needed for error mapping). Also add `"google.golang.org/grpc/metadata"` for the auth header injection.

Full import block after the change:
```go
import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	firestorev1 "github.com/petereon/embyr/gen/go/google/firestore/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)
```

- [ ] **Step 2: Update Handler to carry resolveFn**

Replace the `Handler` struct and `NewHandler` function:

```go
// Handler handles both POST (forward channel) and GET (back channel) for BrowserChannel.
type Handler struct {
	mgr       *Manager
	listenFn  func(firestorev1.Firestore_ListenServer) error
	resolveFn TenantResolveFn // nil in single-tenant mode
}

// NewHandler creates a BrowserChannel Handler. Pass resolveFn=nil in single-tenant mode.
func NewHandler(mgr *Manager, listenFn func(firestorev1.Firestore_ListenServer) error, resolveFn TenantResolveFn) *Handler {
	return &Handler{mgr: mgr, listenFn: listenFn, resolveFn: resolveFn}
}
```

- [ ] **Step 3: Enforce auth in Handler.handleForward for new sessions**

In `handleForward`, the new-session branch (`if sid == "" && rid != ""`). Replace the existing block with:

```go
if sid == "" && rid != "" {
	// Parse body first so we can extract the database path for tenant resolution.
	reqs, _ := parseForwardBody(r)

	ctx := r.Context()
	if h.resolveFn != nil {
		var database string
		if len(reqs) > 0 {
			database = reqs[0].GetDatabase()
		}
		// Inject HTTP Authorization header as gRPC incoming metadata so
		// auth.ValidateForTenant → bearerToken(ctx) works correctly.
		if authHeader := r.Header.Get("Authorization"); authHeader != "" {
			md := metadata.New(map[string]string{"authorization": authHeader})
			ctx = metadata.NewIncomingContext(ctx, md)
		}
		var err error
		ctx, err = h.resolveFn(ctx, database)
		if err != nil {
			c := status.Code(err)
			http.Error(w, status.Convert(err).Message(), wcGRPCToHTTP(c))
			return
		}
	}

	sess := h.mgr.NewSession()
	sessCtx, cancel := context.WithCancel(ctx) // authenticated context propagates to bridge
	bridge := newListenBridge(sessCtx)

	sess.bridge = bridge
	sess.cancel = cancel

	// gRPC handler goroutine.
	go func() {
		defer cancel()
		defer h.mgr.Remove(sess.ID)
		_ = h.listenFn(bridge)
	}()

	// Pump goroutine: reads ListenResponses and appends JSON chunks.
	go func() {
		for {
			select {
			case resp, ok := <-bridge.sendCh:
				if !ok {
					return
				}
				b, err := protojson.Marshal(resp)
				if err != nil {
					log.Printf("[webchannel] listen pump: marshal error (terminating session): %v", err)
					cancel()
					return
				}
				chunk := sess.FormatDataChunk(json.RawMessage(b))
				sess.listenLog.Append(chunk)
			case <-bridge.ctx.Done():
				return
			}
		}
	}()

	for _, req := range reqs {
		select {
		case bridge.recvCh <- req:
		case <-r.Context().Done():
			return
		}
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(sess.FormatConnectChunk())
	return
}
```

Add the helper `wcGRPCToHTTP` at the bottom of `handler.go` (before the `parseForwardBody` function):

```go
func wcGRPCToHTTP(c codes.Code) int {
	switch c {
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	default:
		return http.StatusInternalServerError
	}
}
```

- [ ] **Step 4: Apply same changes to WriteHandler**

Replace `WriteHandler` struct and `NewWriteHandler`:

```go
type WriteHandler struct {
	mgr       *Manager
	writeFn   func(firestorev1.Firestore_WriteServer) error
	resolveFn TenantResolveFn
}

func NewWriteHandler(mgr *Manager, writeFn func(firestorev1.Firestore_WriteServer) error, resolveFn TenantResolveFn) *WriteHandler {
	return &WriteHandler{mgr: mgr, writeFn: writeFn, resolveFn: resolveFn}
}
```

In `WriteHandler.handleForward`, new-session branch, replace:
```go
if sid == "" && rid != "" {
    sess := h.mgr.NewSession()
    reqs, _ := parseWriteForwardBody(r)
    ctx, cancel := context.WithCancel(context.Background())
```

With:
```go
if sid == "" && rid != "" {
	reqs, _ := parseWriteForwardBody(r)

	ctx := r.Context()
	if h.resolveFn != nil {
		var database string
		if len(reqs) > 0 {
			database = reqs[0].GetDatabase()
		}
		if authHeader := r.Header.Get("Authorization"); authHeader != "" {
			md := metadata.New(map[string]string{"authorization": authHeader})
			ctx = metadata.NewIncomingContext(ctx, md)
		}
		var err error
		ctx, err = h.resolveFn(ctx, database)
		if err != nil {
			c := status.Code(err)
			http.Error(w, status.Convert(err).Message(), wcGRPCToHTTP(c))
			return
		}
	}

	sess := h.mgr.NewSession()
	sessCtx, cancel := context.WithCancel(ctx)
```

Then change `bridge := newWriteBridge(ctx)` → `bridge := newWriteBridge(sessCtx)` and `sess.cancel = cancel` stays the same. The pump goroutine and send-loop are identical to before; only the context changes.

- [ ] **Step 5: Update server.go — NewMultiTenant wires resolveFn and uses FactoryLike**

In `internal/server/server.go`:

a) Change the `NewMultiTenant` signature:
```go
func NewMultiTenant(cfg *config.Config, factory tenancy.FactoryLike, log *zap.Logger) (*Server, error) {
```

b) After creating `wcMgr`, build the resolver and pass it:
```go
wcMgr := webchannel.NewManager()
resolver := tenancy.NewResolveFn(factory, log)
wcHandler := webchannel.NewHandler(wcMgr, fs.Listen, resolver)
wcWriteHandler := webchannel.NewWriteHandler(wcMgr, fs.Write, resolver)
```

c) In `server.New` (single-tenant mode), pass `nil` for `resolveFn`:
```go
wcMgr := webchannel.NewManager()
wcHandler := webchannel.NewHandler(wcMgr, fs.Listen, nil)
wcWriteHandler := webchannel.NewWriteHandler(wcMgr, fs.Write, nil)
```

- [ ] **Step 6: Run full test suite**

```bash
go test ./... 2>&1 | tail -30
```

Expected: all tests pass. Any test that creates a `webchannel.NewHandler` or `webchannel.NewWriteHandler` must add `nil` as the third argument — fix those call sites if the compiler reports errors.

- [ ] **Step 7: Commit**

```bash
git add internal/webchannel/handler.go internal/server/server.go
git commit -m "fix: WebChannel sessions inherit authenticated context — closes auth bypass on BrowserChannel surface"
```

---

## Task 5: Server-level auth integration tests

**Files:**
- Create: `internal/server/multitenant_auth_test.go`

These tests start an in-process multi-tenant server with a fake `FactoryLike` backed by sqlite, then send HTTP requests that cover the full security matrix: no token, wrong token, cross-tenant token, correct token. They prove all three bypasses are closed end-to-end.

- [ ] **Step 1: Write the test file**

Create `internal/server/multitenant_auth_test.go`:

```go
package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/petereon/embyr/internal/auth"
	"github.com/petereon/embyr/internal/config"
	"github.com/petereon/embyr/internal/registry"
	"github.com/petereon/embyr/internal/server"
	"github.com/petereon/embyr/internal/store"
	"github.com/petereon/embyr/internal/store/sqlite"
	"github.com/petereon/embyr/internal/tenancy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// mtFactory is an in-memory FactoryLike with named tenants.
type mtFactory struct {
	entries map[string]*mtEntry
}

type mtEntry struct {
	adapter    store.StorageAdapter
	authConfig *auth.Config
	rawCfg     []byte
}

func (f *mtFactory) Get(_ context.Context, projectID, databaseID string) (store.StorageAdapter, *auth.Config, []byte, error) {
	key := projectID + "/" + databaseID
	e, ok := f.entries[key]
	if !ok {
		return nil, nil, nil, registry.ErrTenantNotFound
	}
	return e.adapter, e.authConfig, e.rawCfg, nil
}

func newKeyEntry(t *testing.T, key string) *mtEntry {
	t.Helper()
	dir := t.TempDir()
	adapter, err := sqlite.New(dir+"/test.db", "../../migrations/sqlite")
	require.NoError(t, err)
	require.NoError(t, adapter.Migrate(context.Background()))
	t.Cleanup(func() { adapter.Close() })

	authCfg, err := auth.New(
		&config.Config{Auth: config.AuthConfig{Mode: "key", Key: key}},
		zap.NewNop(),
	)
	require.NoError(t, err)

	rawCfg, _ := json.Marshal(map[string]string{"key": key})
	return &mtEntry{adapter: adapter, authConfig: authCfg, rawCfg: rawCfg}
}

// startMultiTenantServer starts a server.NewMultiTenant with the given factory and
// returns the REST base URL. The server is stopped when t.Cleanup runs.
func startMultiTenantServer(t *testing.T, factory tenancy.FactoryLike) string {
	t.Helper()
	grpcPort := freePort(t)
	restPort := freePort(t)
	cfg := &config.Config{}
	cfg.Server.GRPCPort = grpcPort
	cfg.Server.RESTPort = restPort
	cfg.Transactions.TTL = 60 * time.Second
	cfg.Transactions.SweepInterval = 30 * time.Second

	srv, err := server.NewMultiTenant(cfg, factory, zap.NewNop())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go srv.Run(ctx) //nolint:errcheck

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", restPort)
	require.Eventually(t, func() bool {
		resp, err := http.Get(baseURL + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 50*time.Millisecond, "server did not become ready")

	return baseURL
}

func TestMultiTenant_AuthMatrix(t *testing.T) {
	factory := &mtFactory{entries: map[string]*mtEntry{
		"proj-a/db-a": newKeyEntry(t, "key-for-a"),
		"proj-b/db-b": newKeyEntry(t, "key-for-b"),
	}}
	base := startMultiTenantServer(t, factory)

	pathA := base + "/v1/projects/proj-a/databases/db-a/documents/col/doc"
	pathB := base + "/v1/projects/proj-b/databases/db-b/documents/col/doc"

	cases := []struct {
		name     string
		path     string
		token    string // empty = no Authorization header
		wantCode int
	}{
		{"no_token_tenant_a", pathA, "", http.StatusUnauthorized},
		{"wrong_key_tenant_a", pathA, "Bearer wrong-key", http.StatusUnauthorized},
		{"correct_key_tenant_a", pathA, "Bearer key-for-a", http.StatusNotFound}, // doc doesn't exist → 404
		{"no_token_tenant_b", pathB, "", http.StatusUnauthorized},
		{"wrong_key_tenant_b", pathB, "Bearer wrong-key", http.StatusUnauthorized},
		{"cross_tenant_key_a_path_b", pathB, "Bearer key-for-a", http.StatusUnauthorized},
		{"cross_tenant_key_b_path_a", pathA, "Bearer key-for-b", http.StatusUnauthorized},
		{"correct_key_tenant_b", pathB, "Bearer key-for-b", http.StatusNotFound},
		{"unknown_tenant", base + "/v1/projects/ghost/databases/db/documents/col/doc", "Bearer any-key", http.StatusUnauthorized},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, tc.path, nil)
			require.NoError(t, err)
			if tc.token != "" {
				req.Header.Set("Authorization", tc.token)
			}
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			io.Copy(io.Discard, resp.Body) //nolint:errcheck
			resp.Body.Close()
			assert.Equal(t, tc.wantCode, resp.StatusCode, "case %s", tc.name)
		})
	}
}

func TestMultiTenant_HealthzRequiresNoAuth(t *testing.T) {
	// /healthz is outside tenant routing — must always return 200 with no token.
	factory := &mtFactory{entries: map[string]*mtEntry{}}
	base := startMultiTenantServer(t, factory)

	resp, err := http.Get(base + "/healthz")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
```

- [ ] **Step 2: Run the test to confirm it fails (before implementations are complete)**

At this point Tasks 1–4 are already complete, so the tests should pass. If running this task before the others, you will see failures.

```bash
go test ./internal/server/... -run "TestMultiTenant" -v -timeout 30s
```

Expected after completing Tasks 1–4: all cases in the auth matrix pass.

- [ ] **Step 3: Run full test suite**

```bash
go test ./... -timeout 60s 2>&1 | tail -30
```

Expected: all tests pass, no new failures.

- [ ] **Step 4: Commit**

```bash
git add internal/server/multitenant_auth_test.go
git commit -m "test: server-level auth matrix — proves all three multi-tenant bypass vectors are closed"
```

---

## Self-Review

### Spec coverage

| Bypass | Fix | Task |
|--------|-----|------|
| ParseTenantKey rejects `/v1/` REST URLs | Strip `/v1/` prefix | Task 1 |
| ValidateForTenant never called (gRPC + HTTP) | Call in resolveTenant after lookup | Task 3 |
| HTTP requests bypass auth (no header injection) | Inject Authorization as gRPC metadata | Task 3 |
| WebChannel sessions use context.Background() | Pass authenticated context to bridge | Task 4 |
| AuthInfo.Config always nil | Return raw bytes from factory.Get | Task 2 |
| End-to-end proof | Auth matrix tests | Task 5 |

### Known gaps (out of scope for this plan)

- **gRPC reflection unrestricted**: `reflection.Register(grpcSrv)` in multi-tenant mode allows any caller to enumerate Firestore RPC definitions without a token. Since Firestore API is publicly documented this is low severity. Fix: gate reflection on a mode flag or remove it in multi-tenant mode.
- **CORS AllowCredentials + AllowAll**: `AllowCredentials: true` with wildcard origin is a browser security concern. Addressed by the existing `AllowedOrigins` config option; operators must configure it for production.
- **LRU eviction use-after-close**: evicted adapters are closed with no refcounting. Requests in-flight against an evicted adapter can see nil-deref or closed-pool errors. Low-probability in practice (cache size 200) but a correctness issue.

### Placeholder scan

None found. Every step contains the exact code to write.

### Type consistency

- `FactoryLike.Get` returns `[]byte` for rawAuthConfig throughout (Tasks 2–5). ✓
- `TenantResolveFn` type is `func(ctx context.Context, path string) (context.Context, error)` — matches `NewResolveFn` return and `Handler.resolveFn` usage. ✓
- `stubFactory` / `mtFactory` both implement `FactoryLike` with the correct 4-value return. ✓
