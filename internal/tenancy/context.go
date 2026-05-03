package tenancy

import (
	"context"

	"github.com/petereon/embyr/internal/store"
)

type ctxAdapterKey struct{}

// WithAdapter stores a resolved StorageAdapter in the context.
func WithAdapter(ctx context.Context, a store.StorageAdapter) context.Context {
	return context.WithValue(ctx, ctxAdapterKey{}, a)
}

// AdapterFromCtx retrieves the StorageAdapter injected by tenancy middleware.
// Returns nil if none was injected (single-tenant mode).
func AdapterFromCtx(ctx context.Context) store.StorageAdapter {
	a, _ := ctx.Value(ctxAdapterKey{}).(store.StorageAdapter)
	return a
}

// AuthInfo carries the resolved tenant auth config for the current request.
type AuthInfo struct {
	Mode   string
	Config []byte // raw JSON auth config
}

type ctxAuthKey struct{}

// WithAuthInfo stores AuthInfo in the context.
func WithAuthInfo(ctx context.Context, info AuthInfo) context.Context {
	return context.WithValue(ctx, ctxAuthKey{}, info)
}

// AuthInfoFromCtx retrieves AuthInfo from the context.
func AuthInfoFromCtx(ctx context.Context) (AuthInfo, bool) {
	info, ok := ctx.Value(ctxAuthKey{}).(AuthInfo)
	return info, ok
}
