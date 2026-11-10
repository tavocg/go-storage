package s3_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	storage "github.com/tavocg/go-storage/v3"
	s3backend "github.com/tavocg/go-storage/v3/backends/s3"
	"github.com/tavocg/go-storage/v3/ratelimit"
)

func sdkClient(t *testing.T, handler http.HandlerFunc, attempts int, standardHTTP bool) *awss3.Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	options := awss3.Options{
		BaseEndpoint: aws.String(server.URL), Region: "us-east-1", UsePathStyle: true,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test-secret"}, nil
		}),
		Retryer: retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = attempts
			o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
		}),
	}
	if standardHTTP {
		options.HTTPClient = server.Client()
	}
	return awss3.New(options)
}
func backendWithLimits(t *testing.T, client *awss3.Client, bucket string, limits map[ratelimit.Category]ratelimit.Limit, c ratelimit.Coordinator) *s3backend.Backend {
	t.Helper()
	b, err := s3backend.New(client, bucket, s3backend.WithRateLimits(ratelimit.Options{Scope: "account", Limits: limits, Coordinator: c}))
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func readBody(t *testing.T, b *s3backend.Backend, key string) error {
	t.Helper()
	_, body, err := b.Get(t.Context(), key)
	if err == nil {
		_, readErr := io.Copy(io.Discard, body)
		closeErr := body.Close()
		err = errors.Join(readErr, closeErr)
	}
	return err
}
func listResponse(w http.ResponseWriter, more bool, token string) {
	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprintf(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>%t</IsTruncated><NextContinuationToken>%s</NextContinuationToken></ListBucketResult>`, more, token)
}
func serviceError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, "<Error><Code>%s</Code><Message>injected</Message></Error>", code)
}

func TestCategoriesAndFreeDeletes(t *testing.T) {
	var writes, reads, deletes atomic.Int64
	client := sdkClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "PUT":
			writes.Add(1)
			io.Copy(io.Discard, r.Body)
		case "GET":
			reads.Add(1)
			io.WriteString(w, "data")
		case "DELETE":
			deletes.Add(1)
			w.WriteHeader(204)
		}
	}, 3, true)
	b := backendWithLimits(t, client, "bucket", map[ratelimit.Category]ratelimit.Limit{
		s3backend.ClassA: {Operations: 1, Interval: time.Hour}, s3backend.ClassB: {Operations: 1, Interval: time.Hour},
	}, nil)
	put := func(key string) error {
		return b.Put(t.Context(), storage.Object{Key: key, Size: 4}, strings.NewReader("data"))
	}
	if err := put("one"); err != nil {
		t.Fatal(err)
	}
	if err := put("two"); !errors.Is(err, ratelimit.ErrLimited) || errors.Is(err, storage.ErrOutcomeUnknown) {
		t.Fatalf("blocked initial write: %v", err)
	}
	if err := readBody(t, b, "one"); err != nil {
		t.Fatal(err)
	}
	if err := readBody(t, b, "two"); !errors.Is(err, ratelimit.ErrLimited) {
		t.Fatal(err)
	}
	if _, err := b.List(t.Context()); !errors.Is(err, ratelimit.ErrLimited) {
		t.Fatal("LIST did not share write budget:", err)
	}
	for range 3 {
		if err := b.Delete(t.Context(), "one"); err != nil {
			t.Fatal(err)
		}
	}
	if writes.Load() != 1 || reads.Load() != 1 || deletes.Load() != 3 {
		t.Fatalf("requests: write=%d read=%d delete=%d", writes.Load(), reads.Load(), deletes.Load())
	}
}

func TestEveryPaginationRequestIsAdmitted(t *testing.T) {
	var pages atomic.Int64
	client := sdkClient(t, func(w http.ResponseWriter, r *http.Request) {
		number := pages.Add(1)
		listResponse(w, true, fmt.Sprint(number))
	}, 3, true)
	b := backendWithLimits(t, client, "bucket", map[ratelimit.Category]ratelimit.Limit{s3backend.ClassA: {Operations: 2, Interval: time.Hour}}, nil)
	if objects, err := b.List(t.Context()); !errors.Is(err, ratelimit.ErrLimited) || objects != nil {
		t.Fatalf("partial list returned as success: %v %v", objects, err)
	}
	if pages.Load() != 2 {
		t.Fatalf("sent %d pages, want 2", pages.Load())
	}
}

func TestEverySDKRetryIsAdmitted(t *testing.T) {
	for _, standard := range []bool{true, false} {
		t.Run(fmt.Sprintf("standard-http=%v", standard), func(t *testing.T) {
			var requests atomic.Int64
			client := sdkClient(t, func(w http.ResponseWriter, r *http.Request) {
				if requests.Add(1) < 3 {
					serviceError(w, 503, "SlowDown")
					return
				}
				io.WriteString(w, "ok")
			}, 4, standard)
			b := backendWithLimits(t, client, "bucket", map[ratelimit.Category]ratelimit.Limit{s3backend.ReadRequests: {Operations: 3, Interval: time.Hour}}, nil)
			if err := readBody(t, b, "object"); err != nil {
				t.Fatal(err)
			}
			if err := readBody(t, b, "object"); !errors.Is(err, ratelimit.ErrLimited) {
				t.Fatal(err)
			}
			if requests.Load() != 3 {
				t.Fatalf("requests=%d want=3", requests.Load())
			}
		})
	}
}

func TestConcurrentBudgetsAreSharedAcrossBuckets(t *testing.T) {
	var requests atomic.Int64
	client := sdkClient(t, func(w http.ResponseWriter, r *http.Request) { requests.Add(1); serviceError(w, 404, "NoSuchKey") }, 3, true)
	c := &ratelimit.MemoryCoordinator{}
	limits := map[ratelimit.Category]ratelimit.Limit{s3backend.ClassB: {Operations: 8, Interval: time.Hour}}
	backends := []*s3backend.Backend{backendWithLimits(t, client, "first", limits, c), backendWithLimits(t, client, "second", limits, c)}
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			err := readBody(t, backends[i%2], "missing")
			if errors.Is(err, storage.ErrNotFound) {
				admitted.Add(1)
			} else if !errors.Is(err, ratelimit.ErrLimited) {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if admitted.Load() != 8 || requests.Load() != 8 {
		t.Fatalf("admitted=%d sent=%d", admitted.Load(), requests.Load())
	}
}

func TestInitializationCountsAndInitialRejectionReturnsBytes(t *testing.T) {
	var writes, lists atomic.Int64
	client := sdkClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" {
			writes.Add(1)
			io.Copy(io.Discard, r.Body)
			return
		}
		lists.Add(1)
		listResponse(w, false, "")
	}, 3, true)
	b := backendWithLimits(t, client, "bucket", map[ratelimit.Category]ratelimit.Limit{s3backend.ClassA: {Operations: 1, Interval: time.Hour}}, nil)
	bytesCoordinator := storage.NewMemoryCoordinator()
	options := storage.Options{Scope: "bucket", Coordinator: bytesCoordinator, Limits: storage.Limits{MaxBytes: 10}}
	s, err := storage.New(t.Context(), b, options)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Put(t.Context(), "blocked", strings.NewReader("1234567890"))
	var pending *storage.PendingError
	if !errors.Is(err, ratelimit.ErrLimited) || errors.As(err, &pending) {
		t.Fatalf("initial rejection should be known unchanged: %v", err)
	}
	// A fresh backend with no rate limit shares the byte coordinator. Its
	// successful reservation proves the denied write returned all ten bytes.
	unmetered, err := s3backend.New(client, "bucket")
	if err != nil {
		t.Fatal(err)
	}
	other, err := storage.New(t.Context(), unmetered, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = other.Put(t.Context(), "fits", strings.NewReader("1234567890")); err != nil {
		t.Fatal("byte reservation leaked:", err)
	}
	if writes.Load() != 1 || lists.Load() != 1 {
		t.Fatalf("writes=%d lists=%d", writes.Load(), lists.Load())
	}
	// Failed initialization must not bypass its backend's depleted budget.
	if _, err := storage.New(t.Context(), b, storage.Options{Scope: "new-scope"}); !errors.Is(err, ratelimit.ErrLimited) {
		t.Fatal(err)
	}
}

func TestBlockedRetryKeepsUncertainWriteReservation(t *testing.T) {
	var writes, lists atomic.Int64
	client := sdkClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "PUT":
			writes.Add(1)
			io.Copy(io.Discard, r.Body)
			serviceError(w, 503, "SlowDown")
		case "GET":
			lists.Add(1)
			listResponse(w, false, "")
		case "DELETE":
			w.WriteHeader(204)
		}
	}, 4, true)
	b := backendWithLimits(t, client, "bucket", map[ratelimit.Category]ratelimit.Limit{s3backend.ClassA: {Operations: 2, Interval: time.Hour}}, nil)
	s, err := storage.New(t.Context(), b, storage.Options{Scope: "bucket", Limits: storage.Limits{MaxBytes: 10}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Put(t.Context(), "uncertain", bytes.NewReader(make([]byte, 10)))
	var pending *storage.PendingError
	if !errors.As(err, &pending) || !errors.Is(err, storage.ErrOutcomeUnknown) || !errors.Is(err, ratelimit.ErrLimited) {
		t.Fatalf("lost uncertain outcome: %v", err)
	}
	if _, err = s.Put(t.Context(), "other", strings.NewReader("x")); !errors.Is(err, storage.ErrQuotaExceeded) {
		t.Fatal("uncertain bytes released:", err)
	}
	if err = s.Delete(t.Context(), "uncertain"); !errors.Is(err, storage.ErrObjectBusy) {
		t.Fatal("uncertain key released:", err)
	}
	if writes.Load() != 1 || lists.Load() != 1 {
		t.Fatalf("writes=%d lists=%d", writes.Load(), lists.Load())
	}
}

func TestHTTPRedirectsCannotBypassLimit(t *testing.T) {
	var first, target atomic.Int64
	client := sdkClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/target") {
			target.Add(1)
			io.WriteString(w, "ok")
			return
		}
		first.Add(1)
		http.Redirect(w, r, "/bucket/target", http.StatusTemporaryRedirect)
	}, 3, true)
	b := backendWithLimits(t, client, "bucket", map[ratelimit.Category]ratelimit.Limit{s3backend.ClassB: {Operations: 1, Interval: time.Hour}}, nil)
	if err := readBody(t, b, "redirect"); !errors.Is(err, ratelimit.ErrLimited) {
		t.Fatalf("redirect: %v", err)
	}
	if first.Load() != 1 || target.Load() != 0 {
		t.Fatalf("initial=%d redirected=%d", first.Load(), target.Load())
	}
}

type offlineCoordinator struct{}

func (offlineCoordinator) Allow(context.Context, string, ratelimit.Category, ratelimit.Limit) error {
	return errors.New("offline")
}
func TestRateCoordinatorFailureNeverSendsMutation(t *testing.T) {
	var requests atomic.Int64
	client := sdkClient(t, func(w http.ResponseWriter, r *http.Request) { requests.Add(1) }, 3, true)
	b := backendWithLimits(t, client, "bucket", map[ratelimit.Category]ratelimit.Limit{s3backend.ClassA: {Operations: 1, Interval: time.Hour}}, offlineCoordinator{})
	err := b.Put(t.Context(), storage.Object{Key: "object", Size: 1}, strings.NewReader("x"))
	if !errors.Is(err, ratelimit.ErrCoordination) || errors.Is(err, storage.ErrOutcomeUnknown) || requests.Load() != 0 {
		t.Fatalf("sent=%d err=%v", requests.Load(), err)
	}
}
