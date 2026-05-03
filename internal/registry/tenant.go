package registry

import (
	"encoding/json"
	"time"
)

// CredentialType identifies how the data plane resolves a DSN for a tenant.
type CredentialType string

const (
	CredentialGCPSecret   CredentialType = "gcp_secret"
	CredentialAWSSecret   CredentialType = "aws_secret"
	CredentialEmbyrSecret CredentialType = "embyr_secret"
	CredentialAgent       CredentialType = "agent"
)

// TenantStatus reflects the lifecycle state stored in the registry.
type TenantStatus string

const (
	TenantStatusProvisioning TenantStatus = "provisioning"
	TenantStatusActive       TenantStatus = "active"
	TenantStatusSuspended    TenantStatus = "suspended"
)

// Tenant is the data-plane view of a registered tenant.
// Fields map 1:1 to the tenants table columns needed by the data plane.
type Tenant struct {
	ID             string
	ProjectID      string
	DatabaseID     string
	SchemaName     string
	CredentialType CredentialType
	CredentialRef  string
	AuthMode       string
	AuthConfig     json.RawMessage
	Status         TenantStatus
	UpdatedAt      time.Time
}
