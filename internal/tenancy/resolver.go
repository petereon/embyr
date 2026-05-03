package tenancy

import (
	"context"
	"fmt"

	"github.com/petereon/embyr/internal/registry"
)

// CredentialResolver resolves a Postgres DSN for a tenant.
type CredentialResolver interface {
	Resolve(ctx context.Context) (dsn string, err error)
}

// NewResolver returns the correct CredentialResolver for the tenant's credential type.
func NewResolver(t *registry.Tenant) (CredentialResolver, error) {
	switch t.CredentialType {
	case registry.CredentialGCPSecret:
		return &GCPSecretResolver{resourceName: t.CredentialRef}, nil
	case registry.CredentialAWSSecret:
		return &AWSSecretResolver{secretARN: t.CredentialRef}, nil
	case registry.CredentialEmbyrSecret:
		return &EmbyrSecretResolver{encryptedDSN: t.CredentialRef}, nil
	case registry.CredentialAgent:
		return &AgentResolver{agentID: t.CredentialRef}, nil
	default:
		return nil, fmt.Errorf("tenancy: unknown credential type %q", t.CredentialType)
	}
}
