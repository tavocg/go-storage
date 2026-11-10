package storage_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	storage "github.com/tavocg/go-storage/v2"
	"github.com/tavocg/go-storage/v2/backends/fs"
	"github.com/tavocg/go-storage/v2/internal/storagetest"
)

func filesystem(t *testing.T) storage.Backend {
	t.Helper()
	b, err := fs.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	return b
}

type failedReader struct{}

func (failedReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestKnownFailuresAndCancellationReleaseCapacity(t *testing.T) {
	for _, mode := range []string{"read", "backend", "cancellation"} {
		t.Run(mode, func(t *testing.T) {
			b := filesystem(t)
			temp := t.TempDir()
			injected := errors.New("known safe failure")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wrapper := &storagetest.HookBackend{Backend: b}
			if mode != "read" {
				wrapper.PutHook = func(context.Context, storage.Object, io.ReadSeeker) error {
					if mode == "cancellation" {
						cancel()
						return context.Canceled
					}
					return injected
				}
			}
			s := storagetest.New(t, wrapper, storage.Options{TempDir: temp, Limits: storage.Limits{MaxBytes: 10}})
			var body io.Reader = strings.NewReader("1234567890")
			if mode == "read" {
				body = failedReader{}
			}
			if _, err := s.Put(ctx, "failed", body); err == nil {
				t.Fatal("expected failure")
			}
			wrapper.PutHook = nil
			storagetest.Put(t, s, "fits", 10)
			entries, err := os.ReadDir(temp)
			if err != nil || len(entries) != 0 {
				t.Fatalf("staging leaked: %v %v", entries, err)
			}
			storagetest.AssertBytes(t, b, 10)
		})
	}
}

type finalizationCoordinator struct {
	storage.Coordinator
	fail bool
}
type finalizationReservation struct {
	storage.Reservation
	fail bool
}

func (c *finalizationCoordinator) Reserve(ctx context.Context, m storage.Mutation) (storage.Reservation, error) {
	r, err := c.Coordinator.Reserve(ctx, m)
	if err != nil {
		return nil, err
	}
	fail := c.fail
	c.fail = false
	return &finalizationReservation{Reservation: r, fail: fail}, nil
}
func (r *finalizationReservation) Complete(ctx context.Context, o storage.Outcome) error {
	if r.fail {
		r.fail = false
		return errors.New("coordinator temporarily unavailable")
	}
	return r.Reservation.Complete(ctx, o)
}

func TestFinalizationCanBeRetriedWithoutReupload(t *testing.T) {
	b := filesystem(t)
	c := &finalizationCoordinator{Coordinator: storage.NewMemoryCoordinator(), fail: true}
	s := storagetest.New(t, b, storage.Options{Coordinator: c, Limits: storage.Limits{MaxBytes: 10}})
	_, err := s.Put(t.Context(), "stored", strings.NewReader("1234567890"))
	var pending *storage.PendingError
	if !errors.As(err, &pending) || pending.Suggested != storage.Applied || !errors.Is(err, storage.ErrCoordination) {
		t.Fatalf("pending: %v", err)
	}
	if err := s.Delete(t.Context(), "stored"); !errors.Is(err, storage.ErrObjectBusy) {
		t.Fatalf("lost key ownership: %v", err)
	}
	if err := pending.Reservation.Complete(t.Context(), pending.Suggested); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(t.Context(), "stored"); err != nil {
		t.Fatal(err)
	}
	storagetest.Put(t, s, "fits", 10)
	storagetest.AssertBytes(t, b, 10)
}

func TestUnknownUnchangedCanBeRecovered(t *testing.T) {
	b := filesystem(t)
	wrapper := &storagetest.HookBackend{Backend: b, PutHook: func(context.Context, storage.Object, io.ReadSeeker) error { return storage.ErrOutcomeUnknown }}
	s := storagetest.New(t, wrapper, storage.Options{Limits: storage.Limits{MaxBytes: 10}})
	_, err := s.Put(t.Context(), "uncertain", strings.NewReader("1234567890"))
	var pending *storage.PendingError
	if !errors.As(err, &pending) {
		t.Fatal(err)
	}
	// This injected writer did not publish and has returned; Unchanged is known.
	if err := pending.Reservation.Complete(t.Context(), storage.Unchanged); err != nil {
		t.Fatal(err)
	}
	wrapper.PutHook = nil
	storagetest.Put(t, s, "fits", 10)
}

type unavailableCoordinator struct {
	storage.Coordinator
	denyAllow bool
}

func (c unavailableCoordinator) Allow(ctx context.Context, scope string, op storage.Operation) error {
	if c.denyAllow {
		return errors.New("offline")
	}
	return c.Coordinator.Allow(ctx, scope, op)
}
func (c unavailableCoordinator) Reserve(context.Context, storage.Mutation) (storage.Reservation, error) {
	return nil, errors.New("offline")
}

func TestCoordinatorFailuresFailClosed(t *testing.T) {
	for _, deny := range []bool{true, false} {
		t.Run(map[bool]string{true: "allow", false: "reserve"}[deny], func(t *testing.T) {
			b := filesystem(t)
			c := unavailableCoordinator{Coordinator: storage.NewMemoryCoordinator(), denyAllow: deny}
			s := storagetest.New(t, b, storage.Options{Coordinator: c})
			if _, err := s.Put(t.Context(), "blocked", strings.NewReader("data")); !errors.Is(err, storage.ErrCoordination) {
				t.Fatal(err)
			}
			storagetest.AssertBytes(t, b, 0)
		})
	}
}

func TestDefaultCoordinatorIsPrivate(t *testing.T) {
	b := filesystem(t)
	opts := storage.Options{Scope: "same", Limits: storage.Limits{Rate: storage.RateLimit{Operations: 1, Interval: time.Hour}}}
	first := storagetest.New(t, b, opts)
	second := storagetest.New(t, b, opts)
	if _, err := first.List(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := first.List(t.Context()); !errors.Is(err, storage.ErrRateLimited) {
		t.Fatal(err)
	}
	if _, err := second.List(t.Context()); err != nil {
		t.Fatal("default instance shares hidden global state:", err)
	}
}

func TestValidationAndFailedAttemptsCount(t *testing.T) {
	b := filesystem(t)
	invalid := []storage.Options{
		{}, {Scope: "x", Limits: storage.Limits{MaxBytes: -1}},
		{Scope: "x", Limits: storage.Limits{Rate: storage.RateLimit{Operations: 1}}},
		{Scope: "x", FinalizeTimeout: -time.Second},
	}
	for _, opts := range invalid {
		if _, err := storage.New(t.Context(), b, opts); err == nil {
			t.Fatalf("invalid options accepted: %+v", opts)
		}
	}
	if _, err := storage.New(t.Context(), nil, storage.Options{Scope: "x"}); err == nil {
		t.Fatal("nil backend accepted")
	}
	s := storagetest.New(t, b, storage.Options{Limits: storage.Limits{Rate: storage.RateLimit{Operations: 1, Interval: time.Hour}}})
	if _, err := s.Put(t.Context(), "", bytes.NewReader(nil)); !errors.Is(err, storage.ErrInvalidKey) {
		t.Fatal(err)
	}
	if _, err := s.Put(t.Context(), "read-failure", failedReader{}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	if _, err := s.List(t.Context()); !errors.Is(err, storage.ErrRateLimited) {
		t.Fatal("read failure refunded operation:", err)
	}
}
