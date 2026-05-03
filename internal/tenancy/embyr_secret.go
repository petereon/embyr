package tenancy

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
)

// EmbyrSecretResolver decrypts a DSN blob using Embyr's own GCP Cloud KMS key.
// credential_ref format: "<kms-key-resource>|<base64-ciphertext>"
type EmbyrSecretResolver struct {
	encryptedDSN string
}

func (r *EmbyrSecretResolver) Resolve(ctx context.Context) (string, error) {
	sep := strings.IndexByte(r.encryptedDSN, '|')
	if sep < 0 {
		return "", fmt.Errorf("tenancy: embyr_secret: malformed credential_ref (missing '|')")
	}
	keyResource := r.encryptedDSN[:sep]
	ciphertext, err := base64.StdEncoding.DecodeString(r.encryptedDSN[sep+1:])
	if err != nil {
		return "", fmt.Errorf("tenancy: embyr_secret: base64 decode ciphertext: %w", err)
	}

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
