package health

import (
	"context"
	"net/http"
	"time"
)

// Pinger is the subset of store.StorageAdapter needed by health checks.
// Using a local interface avoids importing the store package here.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Handler holds the health and readiness HTTP handlers.
type Handler struct {
	db Pinger
}

// New returns a Handler that uses db for readiness checks.
func New(db Pinger) *Handler {
	return &Handler{db: db}
}

// Healthz always returns 200 OK — the process is alive.
func (h *Handler) Healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// Readyz returns 200 OK when the database is reachable, 503 otherwise.
func (h *Handler) Readyz(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.db.Ping(ctx); err != nil {
		http.Error(w, "db unreachable: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
