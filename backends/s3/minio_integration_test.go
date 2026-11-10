//go:build integration

package s3_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	storage "github.com/tavocg/go-storage/v2"
	s3backend "github.com/tavocg/go-storage/v2/backends/s3"
	"github.com/tavocg/go-storage/v2/internal/storagetest"
)

func minioBackend(t *testing.T) storage.Backend {
	t.Helper()
	endpoint := os.Getenv("MINIO_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set MINIO_TEST_ENDPOINT to run MinIO integration tests")
	}
	client := awss3.New(awss3.Options{
		BaseEndpoint: &endpoint, Region: "us-east-1", UsePathStyle: true,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "storage-test", SecretAccessKey: "storage-test-secret"}, nil
		}),
	})
	bucket := "quota-test-" + strings.ToLower(rand.Text())
	if _, err := client.CreateBucket(t.Context(), &awss3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatal(err)
	}
	b, err := s3backend.New(client, bucket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		objects, err := b.List(ctx)
		if err != nil {
			t.Error(err)
			return
		}
		for _, o := range objects {
			if err := b.Delete(ctx, o.Key); err != nil {
				t.Error(err)
			}
		}
		if _, err := client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: &bucket}); err != nil {
			t.Error(err)
		}
	})
	return b
}
func TestMinIO(t *testing.T) { storagetest.Run(t, minioBackend) }

func TestMinIOInitializationPaginates(t *testing.T) {
	b := minioBackend(t)
	jobs := make(chan int)
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			for i := range jobs {
				err := b.Put(t.Context(), storage.Object{Key: fmt.Sprintf("seed-%04d", i), Size: 1}, bytes.NewReader([]byte{1}))
				if err != nil {
					t.Error(err)
				}
			}
		})
	}
	for i := range 1005 {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	if t.Failed() {
		return
	}
	s := storagetest.New(t, b, storage.Options{Limits: storage.Limits{MaxBytes: 1005}})
	objects, err := s.List(t.Context())
	if err != nil || len(objects) != 1005 {
		t.Fatalf("pagination: %d objects, %v", len(objects), err)
	}
	if _, err := s.Put(t.Context(), "overflow", bytes.NewReader([]byte{1})); !errors.Is(err, storage.ErrQuotaExceeded) {
		t.Fatalf("initialization missed objects beyond the first page: %v", err)
	}
}
