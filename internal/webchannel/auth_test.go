package webchannel_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	firestorev1 "github.com/petereon/embyr/gen/go/google/firestore/v1"
	"github.com/petereon/embyr/internal/webchannel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rejectAllFn is a TenantResolveFn that always rejects with Unauthenticated.
func rejectAllFn(_ context.Context, _ string) (context.Context, error) {
	return context.Background(), status.Error(codes.Unauthenticated, "unauthenticated")
}

// acceptAllFn is a TenantResolveFn that always accepts and passes context through.
func acceptAllFn(ctx context.Context, _ string) (context.Context, error) {
	return ctx, nil
}

// errFn is a TenantResolveFn that returns a generic internal error.
func errFn(_ context.Context, _ string) (context.Context, error) {
	return context.Background(), status.Error(codes.Internal, "internal error")
}

// blockingListenFn blocks until the bridge context is cancelled. Used to keep
// sessions alive long enough for the test to inspect state.
func blockingListenFn(s firestorev1.Firestore_ListenServer) error {
	<-s.Context().Done()
	return nil
}

// blockingWriteFn blocks until the bridge context is cancelled.
func blockingWriteFn(s firestorev1.Firestore_WriteServer) error {
	<-s.Context().Done()
	return nil
}

// newChannelPOSTRequest builds a minimal forward-channel POST that starts a new
// session. VER=8, RID=1, and no SID triggers the new-session branch.
func newChannelPOSTRequest(t *testing.T, target, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return req
}

// ── Handler (Listen) ──────────────────────────────────────────────────────────

func TestHandler_ResolveFnRejectsUnauthenticated(t *testing.T) {
	mgr := webchannel.NewManager()
	handler := webchannel.NewHandler(mgr, blockingListenFn, rejectAllFn)

	req := newChannelPOSTRequest(t, "/Listen/channel?VER=8&RID=1&CVER=22", "")
	req.Header.Set("Authorization", "Bearer wrong-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Empty(t, mgr.AnySessionID(), "no session should be created on auth failure")
}

func TestHandler_ResolveFnInternalError(t *testing.T) {
	mgr := webchannel.NewManager()
	handler := webchannel.NewHandler(mgr, blockingListenFn, errFn)

	req := newChannelPOSTRequest(t, "/Listen/channel?VER=8&RID=1&CVER=22", "")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Empty(t, mgr.AnySessionID(), "no session should be created on error")
}

func TestHandler_NilResolveFnPassesThrough(t *testing.T) {
	mgr := webchannel.NewManager()
	handler := webchannel.NewHandler(mgr, blockingListenFn, nil)

	req := newChannelPOSTRequest(t, "/Listen/channel?VER=8&RID=1&CVER=22", "")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// nil resolveFn = single-tenant mode: session must be created and response is 200.
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, mgr.AnySessionID(), "session must be registered in single-tenant mode")

	// Clean up: shut down the session so the blocked goroutine exits.
	mgr.Shutdown()
}

func TestHandler_ResolveFnAcceptsAndCreatesSession(t *testing.T) {
	mgr := webchannel.NewManager()
	handler := webchannel.NewHandler(mgr, blockingListenFn, acceptAllFn)

	req := newChannelPOSTRequest(t, "/Listen/channel?VER=8&RID=1&CVER=22", "")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, mgr.AnySessionID(), "session must be registered when resolveFn accepts")

	mgr.Shutdown()
}

// ── WriteHandler (Write) ──────────────────────────────────────────────────────

func TestWriteHandler_ResolveFnRejectsUnauthenticated(t *testing.T) {
	mgr := webchannel.NewManager()
	handler := webchannel.NewWriteHandler(mgr, blockingWriteFn, rejectAllFn)

	req := newChannelPOSTRequest(t, "/Write/channel?VER=8&RID=1&CVER=22", "")
	req.Header.Set("Authorization", "Bearer wrong-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Empty(t, mgr.AnySessionID(), "no session should be created on auth failure")
}

func TestWriteHandler_ResolveFnInternalError(t *testing.T) {
	mgr := webchannel.NewManager()
	handler := webchannel.NewWriteHandler(mgr, blockingWriteFn, errFn)

	req := newChannelPOSTRequest(t, "/Write/channel?VER=8&RID=1&CVER=22", "")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Empty(t, mgr.AnySessionID(), "no session should be created on error")
}

func TestWriteHandler_NilResolveFnPassesThrough(t *testing.T) {
	mgr := webchannel.NewManager()
	handler := webchannel.NewWriteHandler(mgr, blockingWriteFn, nil)

	req := newChannelPOSTRequest(t, "/Write/channel?VER=8&RID=1&CVER=22", "")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// nil resolveFn = single-tenant mode: session must be created and response is 200.
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, mgr.AnySessionID(), "session must be registered in single-tenant mode")

	mgr.Shutdown()
}

func TestWriteHandler_ResolveFnAcceptsAndCreatesSession(t *testing.T) {
	mgr := webchannel.NewManager()
	handler := webchannel.NewWriteHandler(mgr, blockingWriteFn, acceptAllFn)

	req := newChannelPOSTRequest(t, "/Write/channel?VER=8&RID=1&CVER=22", "")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, mgr.AnySessionID(), "session must be registered when resolveFn accepts")

	mgr.Shutdown()
}
