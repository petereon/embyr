package health_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/petereon/firstyr/internal/health"
	"github.com/stretchr/testify/assert"
)

type mockAdapter struct{ pingErr error }

func (m *mockAdapter) Ping(_ context.Context) error    { return m.pingErr }
func (m *mockAdapter) Migrate(_ context.Context) error { return nil }
func (m *mockAdapter) Close() error                    { return nil }

func TestHealthz(t *testing.T) {
	h := health.New(&mockAdapter{})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	h.Healthz(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "ok", w.Body.String())
}

func TestReadyz_DBReachable(t *testing.T) {
	h := health.New(&mockAdapter{})
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	h.Readyz(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "ok", w.Body.String())
}

func TestReadyz_DBUnreachable(t *testing.T) {
	h := health.New(&mockAdapter{pingErr: errors.New("connection refused")})
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	h.Readyz(w, req)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}
