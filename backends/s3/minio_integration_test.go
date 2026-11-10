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
	storage "github.com/tavocg/go-storage/v3"
	s3backend "github.com/tavocg/go-storage/v3/backends/s3"
	"github.com/tavocg/go-storage/v3/internal/storagetest"
	"github.com/tavocg/go-storage/v3/ratelimit"
)

func minioBackend(t *testing.T) storage.Backend {
	b, _ := minioFixture(t)
	return b
}

func minioFixture(t *testing.T, opts ...s3backend.Option) (*s3backend.Backend, *s3backend.Backend) {
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
	b, err := s3backend.New(client, bucket, opts...)
	if err != nil {
		t.Fatal(err)
	}
	cleanupBackend, err := s3backend.New(client, bucket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		objects, err := cleanupBackend.List(ctx)
		if err != nil {
			t.Error(err)
			return
		}
		for _, o := range objects {
			if err := cleanupBackend.Delete(ctx, o.Key); err != nil {
				t.Error(err)
			}
		}
		if _, err := client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: &bucket}); err != nil {
			t.Error(err)
		}
	})
	return b, cleanupBackend
}
func TestMinIO(t *testing.T) { storagetest.Run(t, minioBackend) }

func TestMinIOInitializationPaginates(t *testing.T) {
	b, raw := minioFixture(t, s3backend.WithRateLimits(ratelimit.Options{
		Scope: "paginated-account",
		Limits: map[ratelimit.Category]ratelimit.Limit{
			s3backend.ClassA: {Operations: 2, Interval: time.Hour},
		},
	}))
	jobs := make(chan int)
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			for i := range jobs {
				err := raw.Put(t.Context(), storage.Object{Key: fmt.Sprintf("seed-%04d", i), Size: 1}, bytes.NewReader([]byte{1}))
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
	objects, err := raw.List(t.Context())
	if err != nil || len(objects) != 1005 {
		t.Fatalf("pagination: %d objects, %v", len(objects), err)
	}
	if _, err := s.List(t.Context()); !errors.Is(err, ratelimit.ErrLimited) {
		t.Fatalf("initialization did not consume both page requests: %v", err)
	}
	if _, err := s.Put(t.Context(), "overflow", bytes.NewReader([]byte{1})); !errors.Is(err, storage.ErrQuotaExceeded) {
		t.Fatalf("initialization missed objects beyond the first page: %v", err)
	}
}

func TestMinIOCategoryBudgetsAndFreeDeletes(t *testing.T) {
	limited, raw := minioFixture(t, s3backend.WithRateLimits(ratelimit.Options{Scope: "r2-account", Limits: map[ratelimit.Category]ratelimit.Limit{
		s3backend.ClassA: {Operations: 3, Interval: time.Hour}, s3backend.ClassB: {Operations: 1, Interval: time.Hour},
	}}))
	s := storagetest.New(t, limited, storage.Options{Limits: storage.Limits{MaxBytes: 10}})
	// The initialization list consumes the first Class A operation.
	storagetest.Put(t, s, "first", 4)
	storagetest.Put(t, s, "second", 6)
	_, err := s.Put(t.Context(), "first", bytes.NewReader(make([]byte, 2)))
	var pending *storage.PendingError
	if !errors.Is(err, ratelimit.ErrLimited) || errors.As(err, &pending) {
		t.Fatalf("initial rejection: %v", err)
	}
	if err := readBody(t, limited, "first"); err != nil {
		t.Fatal(err)
	}
	if err := readBody(t, limited, "second"); !errors.Is(err, ratelimit.ErrLimited) {
		t.Fatal(err)
	}
	if err := s.Delete(t.Context(), "first"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(t.Context(), "first"); err != nil {
		t.Fatal(err)
	}
	storagetest.AssertBytes(t, raw, 6)
}
