package tenancy

import (
	"context"
	"fmt"
	"os"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GCPSecretResolver fetches a DSN from GCP Secret Manager.
// The customer stores their DSN in their own project and grants
// Embyr's service account secretmanager.secretAccessor on that secret.
type GCPSecretResolver struct {
	resourceName string
}

func (r *GCPSecretResolver) Resolve(ctx context.Context) (string, error) {
	var opts []option.ClientOption
	if host := os.Getenv("EMBYR_GCP_SECRETMANAGER_EMULATOR_HOST"); host != "" {
		conn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return "", fmt.Errorf("tenancy: gcp emulator dial %q: %w", host, err)
		}
		defer conn.Close()
		opts = append(opts, option.WithGRPCConn(conn))
	}

	client, err := secretmanager.NewClient(ctx, opts...)
	if err != nil {
		return "", fmt.Errorf("tenancy: gcp secret manager client: %w", err)
	}
	defer client.Close()

	resp, err := client.AccessSecretVersion(ctx,
		&secretmanagerpb.AccessSecretVersionRequest{Name: r.resourceName})
	if err != nil {
		return "", fmt.Errorf("tenancy: access gcp secret %q: %w", r.resourceName, err)
	}
	if resp.Payload == nil || len(resp.Payload.Data) == 0 {
		return "", fmt.Errorf("tenancy: gcp secret %q returned empty payload", r.resourceName)
	}
	return string(resp.Payload.Data), nil
}
