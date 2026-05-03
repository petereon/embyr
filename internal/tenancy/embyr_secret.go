package tenancy

import (
	"context"
	"fmt"

	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
)

// EmbyrSecretResolver decrypts a DSN blob using Embyr's own GCP Cloud KMS key.
// credential_ref format: "<kms-key-resource>|<base64-ciphertext>"
type EmbyrSecretResolver struct {
	encryptedDSN string
}

func (r *EmbyrSecretResolver) Resolve(ctx context.Context) (string, error) {
	sep := -1
	for i, c := range r.encryptedDSN {
		if c == '|' {
			sep = i
			break
		}
	}
	if sep < 0 {
		return "", fmt.Errorf("tenancy: embyr_secret: malformed credential_ref (missing '|')")
	}
	keyResource := r.encryptedDSN[:sep]
	ciphertext := []byte(r.encryptedDSN[sep+1:])

	client, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		return "", fmt.Errorf("tenancy: kms client: %w", err)
	}
	defer client.Close()

	resp, err := client.Decrypt(ctx, &kmspb.DecryptRequest{
		Name:       keyResource,
		Ciphertext: ciphertext,
	})
	if err != nil {
		return "", fmt.Errorf("tenancy: kms decrypt: %w", err)
	}
	return string(resp.Plaintext), nil
}
