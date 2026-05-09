package tenancy_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/petereon/embyr/internal/registry"
	"github.com/petereon/embyr/internal/tenancy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRegistry is an in-memory Client for tests.
type fakeRegistry struct {
	tenants map[string]*registry.Tenant
}

func (f *fakeRegistry) Get(_ context.Context, projectID, databaseID string) (*registry.Tenant, error) {
	t, ok := f.tenants[projectID+"\x00"+databaseID]
	if !ok {
		return nil, registry.ErrTenantNotFound
	}
	return t, nil
}
func (f *fakeRegistry) Close() {}

func TestAdapterFactory_UnknownTenant(t *testing.T) {
	reg := &fakeRegistry{tenants: map[string]*registry.Tenant{}}
	factory := tenancy.NewAdapterFactory(reg, 10)
	defer factory.Close()

	_, _, _, err := factory.Get(context.Background(), "nope", "nope")
	assert.ErrorIs(t, err, registry.ErrTenantNotFound)
}

func TestAdapterFactory_SuspendedTenant(t *testing.T) {
	reg := &fakeRegistry{tenants: map[string]*registry.Tenant{
		"acme\x00prod": {
			ID: "t1", ProjectID: "acme", DatabaseID: "prod",
			Status: registry.TenantStatusSuspended,
		},
	}}
	factory := tenancy.NewAdapterFactory(reg, 10)
	defer factory.Close()

	_, _, _, err := factory.Get(context.Background(), "acme", "prod")
	assert.ErrorIs(t, err, tenancy.ErrTenantSuspended)
}

func TestAdapterFactory_UnknownCredentialType(t *testing.T) {
	reg := &fakeRegistry{tenants: map[string]*registry.Tenant{
		"acme\x00prod": {
			ID: "t1", ProjectID: "acme", DatabaseID: "prod",
			Status:         registry.TenantStatusActive,
			CredentialType: "bogus",
			CredentialRef:  "ref",
			AuthMode:       "none",
			AuthConfig:     json.RawMessage(`{}`),
		},
	}}
	factory := tenancy.NewAdapterFactory(reg, 10)
	defer factory.Close()

	_, _, _, err := factory.Get(context.Background(), "acme", "prod")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown credential type")
}
