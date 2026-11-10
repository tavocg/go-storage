package fs_test

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	storage "github.com/tavocg/go-storage/v3"
	"github.com/tavocg/go-storage/v3/backends/fs"
	"github.com/tavocg/go-storage/v3/internal/storagetest"
	"github.com/tavocg/go-storage/v3/ratelimit"
)

func limitedFilesystem(t *testing.T, root string, operations int, c ratelimit.Coordinator) *fs.Backend {
	t.Helper()
	b, err := fs.New(root, fs.WithRateLimits(ratelimit.Options{Scope: "directory", Coordinator: c, Limits: map[ratelimit.Category]ratelimit.Limit{fs.Operations: {Operations: operations, Interval: time.Hour}}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}
func TestSharedConcurrentFilesystemLimit(t *testing.T) {
	root := t.TempDir()
	c := &ratelimit.MemoryCoordinator{}
	backends := []*fs.Backend{limitedFilesystem(t, root, 8, c), limitedFilesystem(t, root, 8, c)}
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			_, _, err := backends[i%2].Get(t.Context(), "missing")
			if errors.Is(err, storage.ErrNotFound) {
				admitted.Add(1)
			} else if !errors.Is(err, ratelimit.ErrLimited) {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if admitted.Load() != 8 {
		t.Fatalf("admitted=%d", admitted.Load())
	}
}
func TestFilesystemListCountsOnceAndBlockedPutPreservesObject(t *testing.T) {
	root := t.TempDir()
	unlimited, err := fs.New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer unlimited.Close()
	s := storagetest.New(t, unlimited, storage.Options{})
	for _, key := range []string{"one", "two", "three"} {
		storagetest.Put(t, s, key, 1)
	}
	limited := limitedFilesystem(t, root, 1, nil)
	objects, err := limited.List(t.Context())
	if err != nil || len(objects) != 3 {
		t.Fatalf("list: %v %v", objects, err)
	}
	if err := limited.Put(t.Context(), storage.Object{Key: "one", Size: 2}, bytes.NewReader([]byte("xx"))); !errors.Is(err, ratelimit.ErrLimited) {
		t.Fatal(err)
	}
	_, body, err := unlimited.Get(t.Context(), "one")
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil || len(data) != 1 {
		t.Fatalf("blocked overwrite changed object: %q %v", data, err)
	}
}
func TestFilesystemDefaultLimitsArePrivate(t *testing.T) {
	root := t.TempDir()
	one := limitedFilesystem(t, root, 1, nil)
	two := limitedFilesystem(t, root, 1, nil)
	if _, err := one.List(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := one.List(t.Context()); !errors.Is(err, ratelimit.ErrLimited) {
		t.Fatal(err)
	}
	if _, err := two.List(t.Context()); err != nil {
		t.Fatal(err)
	}
}
