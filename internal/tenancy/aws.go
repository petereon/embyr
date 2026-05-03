package tenancy

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

// AWSSecretResolver fetches a DSN from AWS Secrets Manager.
// The customer stores their DSN in their own account and grants
// Embyr's IAM role secretsmanager:GetSecretValue on that secret.
type AWSSecretResolver struct {
	secretARN string
}

func (r *AWSSecretResolver) Resolve(ctx context.Context) (string, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", fmt.Errorf("tenancy: aws config: %w", err)
	}
	client := secretsmanager.NewFromConfig(cfg)
	out, err := client.GetSecretValue(ctx,
		&secretsmanager.GetSecretValueInput{SecretId: &r.secretARN})
	if err != nil {
		return "", fmt.Errorf("tenancy: get aws secret %q: %w", r.secretARN, err)
	}
	if out.SecretString == nil {
		return "", fmt.Errorf("tenancy: aws secret %q has no string value", r.secretARN)
	}
	return *out.SecretString, nil
}
