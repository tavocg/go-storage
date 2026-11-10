package storage

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemoryInitializeOnceAndPreserveReservations(t *testing.T) {
	c := NewMemoryCoordinator()
	var scans atomic.Int64
	scan := func(context.Context) (map[string]int64, error) { scans.Add(1); return map[string]int64{"old": 4}, nil }
	limits := Limits{MaxBytes: 10}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			if err := c.Initialize(t.Context(), "bucket", limits, scan); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if scans.Load() != 1 {
		t.Fatalf("scans=%d", scans.Load())
	}
	r, err := c.Reserve(t.Context(), Mutation{Scope: "bucket", ID: "first", Key: "new", Operation: OperationPut, Size: 6})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Initialize(t.Context(), "bucket", limits, scan); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Reserve(t.Context(), Mutation{Scope: "bucket", ID: "second", Key: "other", Operation: OperationPut, Size: 1}); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("reservation reset: %v", err)
	}
	if err = r.Complete(t.Context(), Unchanged); err != nil {
		t.Fatal(err)
	}
	if err = c.Initialize(t.Context(), "bucket", Limits{MaxBytes: 11}, scan); !errors.Is(err, ErrConflictingLimits) {
		t.Fatalf("conflicting config: %v", err)
	}
}

func TestMemoryRateWindowAndScopeIsolation(t *testing.T) {
	c := NewMemoryCoordinator()
	limits := Limits{Rate: RateLimit{Operations: 1, Interval: time.Hour}}
	empty := func(context.Context) (map[string]int64, error) { return nil, nil }
	for _, scope := range []string{"one", "two"} {
		if err := c.Initialize(t.Context(), scope, limits, empty); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Allow(t.Context(), "one", OperationGet); err != nil {
		t.Fatal(err)
	}
	var limited *RateLimitError
	if err := c.Allow(t.Context(), "one", OperationPut); !errors.As(err, &limited) || limited.RetryAfter <= 0 {
		t.Fatalf("rate error: %v", err)
	}
	if err := c.Allow(t.Context(), "two", OperationGet); err != nil {
		t.Fatal("scopes share limit:", err)
	}
	// Move the recorded event outside the window without a timing-sensitive sleep.
	c.mu.Lock()
	c.scopes["one"].calls[0] = time.Now().Add(-2 * time.Hour)
	c.mu.Unlock()
	if err := c.Allow(t.Context(), "one", OperationList); err != nil {
		t.Fatal("window did not expire:", err)
	}
}

func TestMemoryOverflowAndInitiallyOverQuota(t *testing.T) {
	c := NewMemoryCoordinator()
	scan := func(context.Context) (map[string]int64, error) { return map[string]int64{"old": math.MaxInt64}, nil }
	if err := c.Initialize(t.Context(), "unlimited", Limits{}, scan); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reserve(t.Context(), Mutation{Scope: "unlimited", ID: "overflow", Key: "new", Operation: OperationPut, Size: 1}); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatal("overflow accepted:", err)
	}
	if err := c.Initialize(t.Context(), "limited", Limits{MaxBytes: 10}, scan); err != nil {
		t.Fatal(err)
	}
	r, err := c.Reserve(t.Context(), Mutation{Scope: "limited", ID: "shrink", Key: "old", Operation: OperationPut, Size: 2})
	if err != nil {
		t.Fatal("cannot shrink over-quota object:", err)
	}
	if err = r.Complete(t.Context(), Applied); err != nil {
		t.Fatal(err)
	}
	r, err = c.Reserve(t.Context(), Mutation{Scope: "limited", ID: "fill", Key: "new", Operation: OperationPut, Size: 8})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Complete(t.Context(), Applied); err != nil {
		t.Fatal(err)
	}
	if c.scopes["limited"].used != 10 {
		t.Fatal("wrong accounting")
	}
}

func TestMemoryCanceledCompletionRetainsReservation(t *testing.T) {
	c := NewMemoryCoordinator()
	if err := c.Initialize(t.Context(), "scope", Limits{MaxBytes: 1}, func(context.Context) (map[string]int64, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	r, err := c.Reserve(t.Context(), Mutation{Scope: "scope", ID: "one", Key: "one", Operation: OperationPut, Size: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err = r.Complete(ctx, Unchanged); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err = c.Reserve(t.Context(), Mutation{Scope: "scope", ID: "two", Key: "two", Operation: OperationPut, Size: 1}); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatal("cancellation freed quota:", err)
	}
	if err = r.Complete(t.Context(), Unchanged); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryInitializationDoesNotBlockOtherScopes(t *testing.T) {
	c := NewMemoryCoordinator()
	empty := func(context.Context) (map[string]int64, error) { return nil, nil }
	if err := c.Initialize(t.Context(), "ready", Limits{}, empty); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- c.Initialize(t.Context(), "loading", Limits{}, func(context.Context) (map[string]int64, error) { close(started); <-release; return nil, nil })
	}()
	<-started
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := c.Initialize(ctx, "loading", Limits{}, empty); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	allowed := make(chan error, 1)
	go func() { allowed <- c.Allow(t.Context(), "ready", OperationGet) }()
	select {
	case err := <-allowed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("scan blocked another scope")
	}
}
