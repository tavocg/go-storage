package ratelimit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStrictSlidingWindow(t *testing.T) {
	now := time.Unix(10000, 0)
	c := &MemoryCoordinator{now: func() time.Time { return now }}
	limit := Limit{Operations: 2, Interval: 4 * time.Hour}
	allow := func() {
		t.Helper()
		if err := c.Allow(t.Context(), "account", "A", limit); err != nil {
			t.Fatal(err)
		}
	}
	deny := func(want time.Duration) {
		t.Helper()
		var e *Error
		err := c.Allow(t.Context(), "account", "A", limit)
		if !errors.As(err, &e) || e.RetryAfter != want {
			t.Fatalf("retry=%+v err=%v want=%s", e, err, want)
		}
	}
	allow()
	now = now.Add(time.Hour)
	allow()
	now = now.Add(time.Hour)
	deny(2 * time.Hour)
	now = now.Add(2 * time.Hour)
	allow()
	deny(time.Hour)
	now = now.Add(time.Hour)
	allow()
	deny(3 * time.Hour)
}

func TestConcurrentSharedBudgetAndIndependentCategories(t *testing.T) {
	c := &MemoryCoordinator{}
	opts := Options{Scope: "account", Coordinator: c, Limits: map[Category]Limit{"A": {Operations: 8, Interval: time.Hour}, "B": {Operations: 1, Interval: time.Hour}}}
	first, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			l := first
			if i%2 == 0 {
				l = second
			}
			err := l.Allow(t.Context(), "A")
			if err == nil {
				admitted.Add(1)
			} else if !errors.Is(err, ErrLimited) {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if admitted.Load() != 8 {
		t.Fatalf("admitted=%d", admitted.Load())
	}
	if err := first.Allow(t.Context(), "B"); err != nil {
		t.Fatal("A consumed B:", err)
	}
	if err := second.Allow(t.Context(), "B"); !errors.Is(err, ErrLimited) {
		t.Fatal(err)
	}
	// Free or unconfigured operations are independent of exhausted categories.
	for range 10 {
		if err := first.Allow(t.Context(), "free"); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Allow(t.Context(), "other-account", "A", opts.Limits["A"]); err != nil {
		t.Fatal(err)
	}
}

func TestConfigurationCopyConflictsAndCancellation(t *testing.T) {
	options := Options{Scope: "account", Coordinator: &MemoryCoordinator{}, Limits: map[Category]Limit{"A": {Operations: 1, Interval: time.Hour}}}
	l, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	options.Limits["A"] = Limit{Operations: 2, Interval: time.Hour}
	if err := l.Allow(t.Context(), "A"); err != nil {
		t.Fatal(err)
	}
	if err := l.Allow(t.Context(), "A"); !errors.Is(err, ErrLimited) {
		t.Fatal("caller changed policy:", err)
	}
	conflicting, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := conflicting.Allow(t.Context(), "A"); !errors.Is(err, ErrConflictingLimits) {
		t.Fatal(err)
	}
	options.Limits["A"] = Limit{}
	disabled, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := disabled.Allow(t.Context(), "A"); !errors.Is(err, ErrConflictingLimits) {
		t.Fatal("explicitly disabling a shared budget bypassed policy verification:", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := l.Allow(ctx, "free"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, bad := range []Options{
		{Limits: map[Category]Limit{"A": {Operations: 1, Interval: time.Hour}}},
		{Scope: "x", Limits: map[Category]Limit{"": {Operations: 1, Interval: time.Hour}}},
		{Scope: "x", Limits: map[Category]Limit{"A": {Operations: 1}}},
		{Scope: "x", Limits: map[Category]Limit{"A": {Operations: -1, Interval: time.Hour}}},
	} {
		if _, err := New(bad); err == nil {
			t.Fatalf("invalid configuration: %+v", bad)
		}
	}
}
