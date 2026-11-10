package storage_test

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	storage "github.com/tavocg/go-storage/v3"
	s3backend "github.com/tavocg/go-storage/v3/backends/s3"
	"github.com/tavocg/go-storage/v3/ratelimit"
)

// This example is compiled but not run: the application supplies an existing
// R2 bucket, credentials, and persistent coordinators for distributed budgets.
func Example_cloudflareR2() {
	newR2 := func(ctx context.Context, accountID, bucket, accessKey, secretKey string, bytesCoordinator storage.Coordinator, requestsCoordinator ratelimit.Coordinator) (*storage.Storage, error) {
		client := awss3.New(awss3.Options{
			Region: "auto", BaseEndpoint: aws.String(fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountID)), UsePathStyle: true,
			RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
			Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
				return aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey}, nil
			}),
		})
		backend, err := s3backend.New(client, bucket, s3backend.WithRateLimits(ratelimit.Options{
			Scope: "r2:" + accountID, Coordinator: requestsCoordinator,
			Limits: map[ratelimit.Category]ratelimit.Limit{
				s3backend.ClassA: {Operations: 10_000, Interval: 8 * time.Hour},
				s3backend.ClassB: {Operations: 100_000, Interval: 8 * time.Hour},
			},
		}))
		if err != nil {
			return nil, err
		}
		return storage.New(ctx, backend, storage.Options{Scope: "r2:" + accountID + ":" + bucket, Coordinator: bytesCoordinator, Limits: storage.Limits{MaxBytes: 9_000_000_000}})
	}
	_ = newR2
}
