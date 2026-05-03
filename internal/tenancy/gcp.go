package tenancy

import (
	"context"
	"fmt"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
)

// GCPSecretResolver fetches a DSN from GCP Secret Manager.
// The customer stores their DSN in their own project and grants
// Embyr's service account secretmanager.secretAccessor on that secret.
type GCPSecretResolver struct {
	resourceName string
}

func (r *GCPSecretResolver) Resolve(ctx context.Context) (string, error) {
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return "", fmt.Errorf("tenancy: gcp secret manager client: %w", err)
	}
	defer client.Close()

	resp, err := client.AccessSecretVersion(ctx,
		&secretmanagerpb.AccessSecretVersionRequest{Name: r.resourceName})
	if err != nil {
		return "", fmt.Errorf("tenancy: access gcp secret %q: %w", r.resourceName, err)
	}
	return string(resp.Payload.Data), nil
}
