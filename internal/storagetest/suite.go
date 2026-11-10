// Package storagetest contains backend-independent integration assertions.
package storagetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	storage "github.com/tavocg/go-storage/v3"
)

type Factory func(*testing.T) storage.Backend

func New(t *testing.T, b storage.Backend, opts storage.Options) *storage.Storage {
	t.Helper()
	if opts.Scope == "" {
		opts.Scope = "test"
	}
	s, err := storage.New(t.Context(), b, opts)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func Put(t *testing.T, s *storage.Storage, key string, size int) storage.Object {
	t.Helper()
	o, err := s.Put(t.Context(), key, bytes.NewReader(make([]byte, size)))
	if err != nil {
		t.Fatal(err)
	}
	return o
}
func AssertBytes(t *testing.T, b storage.Backend, want int64) {
	t.Helper()
	objects, err := b.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, o := range objects {
		total += o.Size
	}
	if total != want {
		t.Fatalf("backend bytes=%d, want=%d: %+v", total, want, objects)
	}
}

// Run executes the same quota and concurrency checks against every backend.
func Run(t *testing.T, factory Factory) {
	t.Run("roundtrip", func(t *testing.T) {
		b := factory(t)
		s := New(t, b, storage.Options{})
		payload := []byte("hello storage")
		object, err := s.Put(t.Context(), "nested/object", bytes.NewBuffer(payload), storage.WithContentType("text/plain"))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(payload)
		if object.Size != int64(len(payload)) || object.SHA256 != hex.EncodeToString(digest[:]) {
			t.Fatalf("metadata: %+v", object)
		}
		got, body, err := s.Get(t.Context(), object.Key)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(body)
		closeErr := body.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(data, payload) || got != object {
			t.Fatalf("get: %+v %q %v %v", got, data, readErr, closeErr)
		}
		objects, err := s.List(t.Context())
		if err != nil || len(objects) != 1 || objects[0].Size != object.Size {
			t.Fatalf("list: %+v %v", objects, err)
		}
		Put(t, s, "empty", 0)
		if err := s.Delete(t.Context(), object.Key); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.Get(t.Context(), object.Key); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("missing: %v", err)
		}
		AssertBytes(t, b, 0)
	})
	t.Run("parallel-unique", func(t *testing.T) {
		const size = 64 << 10
		b := factory(t)
		c := storage.NewMemoryCoordinator()
		opts := storage.Options{Scope: "shared", Coordinator: c, Limits: storage.Limits{MaxBytes: 8 * size}}
		stores := []*storage.Storage{New(t, b, opts), New(t, b, opts)}
		var accepted atomic.Int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range 64 {
			wg.Go(func() {
				<-start
				// bytes.Buffer has no Seek; the library must stage it correctly.
				_, err := stores[i%2].Put(t.Context(), fmt.Sprintf("object-%d", i), bytes.NewBuffer(make([]byte, size)))
				if err == nil {
					accepted.Add(1)
				} else if !errors.Is(err, storage.ErrQuotaExceeded) {
					t.Errorf("put: %v", err)
				}
			})
		}
		close(start)
		wg.Wait()
		if accepted.Load() != 8 {
			t.Fatalf("accepted=%d, want=8", accepted.Load())
		}
		AssertBytes(t, b, 8*size)
	})
	t.Run("repeated-delete", func(t *testing.T) {
		b := factory(t)
		s := New(t, b, storage.Options{Limits: storage.Limits{MaxBytes: 10}})
		Put(t, s, "deleted", 4)
		Put(t, s, "kept", 4)
		for range 2 {
			if err := s.Delete(t.Context(), "deleted"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Put(t.Context(), "extra", bytes.NewBuffer(make([]byte, 7))); !errors.Is(err, storage.ErrQuotaExceeded) {
			t.Fatalf("overflow: %v", err)
		}
		Put(t, s, "extra", 6)
		AssertBytes(t, b, 10)
	})
	t.Run("same-key-serialization", func(t *testing.T) {
		b := factory(t)
		started := make(chan struct{})
		release := make(chan struct{})
		gated := &HookBackend{Backend: b}
		s := New(t, gated, storage.Options{Limits: storage.Limits{MaxBytes: 10}})
		Put(t, s, "same", 8)
		gated.PutHook = func(ctx context.Context, o storage.Object, body io.ReadSeeker) error {
			if o.Key == "same" {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return b.Put(ctx, o, body)
		}
		done := make(chan error, 1)
		go func() { _, err := s.Put(t.Context(), "same", bytes.NewBuffer(make([]byte, 2))); done <- err }()
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			close(release)
			t.Fatal("upload did not start")
		}
		// Always unblock the writer, including after a failed assertion.
		released := false
		defer func() {
			if !released {
				close(release)
				if err := <-done; err != nil {
					t.Error(err)
				}
			}
		}()
		if _, err := s.Put(t.Context(), "same", bytes.NewBuffer(make([]byte, 2))); !errors.Is(err, storage.ErrObjectBusy) {
			t.Errorf("overwrite: %v", err)
		}
		if err := s.Delete(t.Context(), "same"); !errors.Is(err, storage.ErrObjectBusy) {
			t.Errorf("delete: %v", err)
		}
		if _, err := s.Put(t.Context(), "extra", bytes.NewBuffer(make([]byte, 3))); !errors.Is(err, storage.ErrQuotaExceeded) {
			t.Errorf("old bytes released early: %v", err)
		}
		Put(t, s, "fits", 2)
		AssertBytes(t, b, 10)
		close(release)
		released = true
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		AssertBytes(t, b, 4)
		Put(t, s, "released-capacity", 6)
		AssertBytes(t, b, 10)
	})
	t.Run("unknown-outcome", func(t *testing.T) {
		b := factory(t)
		wrapper := &HookBackend{Backend: b, PutHook: func(ctx context.Context, o storage.Object, r io.ReadSeeker) error {
			if err := b.Put(ctx, o, r); err != nil {
				return err
			}
			return storage.ErrOutcomeUnknown
		}}
		s := New(t, wrapper, storage.Options{Limits: storage.Limits{MaxBytes: 10}})
		_, err := s.Put(t.Context(), "uncertain", bytes.NewBuffer(make([]byte, 8)))
		var pending *storage.PendingError
		if !errors.As(err, &pending) || !errors.Is(err, storage.ErrOutcomeUnknown) {
			t.Fatalf("pending: %v", err)
		}
		if _, err = s.Put(t.Context(), "other", bytes.NewBuffer(make([]byte, 3))); !errors.Is(err, storage.ErrQuotaExceeded) {
			t.Fatalf("reservation lost: %v", err)
		}
		if err = s.Delete(t.Context(), "uncertain"); !errors.Is(err, storage.ErrObjectBusy) {
			t.Fatalf("key unlocked: %v", err)
		}
		// The test writer has returned and its actual committed state is known.
		if err = pending.Reservation.Complete(t.Context(), storage.Applied); err != nil {
			t.Fatal(err)
		}
		if err = pending.Reservation.Complete(t.Context(), storage.Applied); err != nil {
			t.Fatal("not idempotent:", err)
		}
		if err = pending.Reservation.Complete(t.Context(), storage.Unchanged); !errors.Is(err, storage.ErrInvalidReservation) {
			t.Fatal("conflicting outcome accepted")
		}
		if err = s.Delete(t.Context(), "uncertain"); err != nil {
			t.Fatal(err)
		}
		AssertBytes(t, b, 0)
	})
	t.Run("existing-state-and-shrink", func(t *testing.T) {
		b := factory(t)
		first := New(t, b, storage.Options{})
		Put(t, first, "existing", 8)
		s := New(t, b, storage.Options{Limits: storage.Limits{MaxBytes: 10}})
		if _, err := s.Put(t.Context(), "extra", bytes.NewBuffer(make([]byte, 3))); !errors.Is(err, storage.ErrQuotaExceeded) {
			t.Fatal(err)
		}
		Put(t, s, "existing", 2)
		Put(t, s, "extra", 8)
		AssertBytes(t, b, 10)
	})
	t.Run("bounded-upload-preserves-old-object", func(t *testing.T) {
		b := factory(t)
		s := New(t, b, storage.Options{Limits: storage.Limits{MaxBytes: 10}})
		Put(t, s, "same", 4)
		if _, err := s.Put(t.Context(), "same", bytes.NewBuffer(make([]byte, 5)), storage.WithSizeLimit(3)); !errors.Is(err, storage.ErrObjectTooLarge) {
			t.Fatalf("oversize: %v", err)
		}
		if _, err := s.Put(t.Context(), "same", bytes.NewBuffer(make([]byte, 11))); !errors.Is(err, storage.ErrQuotaExceeded) {
			t.Fatalf("quota: %v", err)
		}
		AssertBytes(t, b, 4)
		Put(t, s, "empty", 0)
	})
}

// HookBackend injects deterministic failures or pauses around real backends.
// Configure hooks before starting concurrent operations.
type HookBackend struct {
	storage.Backend
	PutHook    func(context.Context, storage.Object, io.ReadSeeker) error
	DeleteHook func(context.Context, string) error
}

func (b *HookBackend) Put(ctx context.Context, o storage.Object, r io.ReadSeeker) error {
	if b.PutHook != nil {
		return b.PutHook(ctx, o, r)
	}
	return b.Backend.Put(ctx, o, r)
}
func (b *HookBackend) Delete(ctx context.Context, key string) error {
	if b.DeleteHook != nil {
		return b.DeleteHook(ctx, key)
	}
	return b.Backend.Delete(ctx, key)
}
