// Package fs implements an atomic filesystem backend. Objects are stored in
// a private file format, with metadata and body in the same renamed file.
package fs

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	storage "github.com/tavocg/go-storage/v3"
	"github.com/tavocg/go-storage/v3/ratelimit"
)

// Backend owns an open filesystem root. Call Close after all users stop.
// The directory must be dedicated to this backend, not a legacy fs backend.
// os.Root confines filesystem operations to the selected directory.
type Backend struct {
	root    *os.Root
	limiter *ratelimit.Limiter
}

// Operations counts each public backend Put, Get, Delete or List once.
const Operations ratelimit.Category = "fs:operation"

type options struct{ rates ratelimit.Options }
type Option func(*options)

func WithRateLimits(rates ratelimit.Options) Option { return func(o *options) { o.rates = rates } }

func New(directory string, opts ...Option) (*Backend, error) {
	var config options
	for _, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("fs: nil option")
		}
		opt(&config)
	}
	for category := range config.rates.Limits {
		if category != Operations {
			return nil, fmt.Errorf("fs: unknown category %q", category)
		}
	}
	limiter, err := ratelimit.New(config.rates)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(directory) == "" {
		return nil, fmt.Errorf("fs: directory is required")
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	return &Backend{root: root, limiter: limiter}, nil
}
func (b *Backend) Close() error { return b.root.Close() }

func filename(key string) string {
	digest := sha256.Sum256([]byte(key))
	return "obj-" + hex.EncodeToString(digest[:])
}

type contextReader struct {
	ctx  context.Context
	body io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.body.Read(p)
}

func (b *Backend) Put(ctx context.Context, object storage.Object, body io.ReadSeeker) error {
	if err := storage.ValidateKey(object.Key); err != nil {
		return err
	}
	if object.Size < 0 || body == nil {
		return fmt.Errorf("fs: invalid body or size")
	}
	if err := b.limiter.Allow(ctx, Operations); err != nil {
		return err
	}
	header, err := json.Marshal(object)
	if err != nil {
		return err
	}
	if len(header)+1 > 16*1024 {
		return fmt.Errorf("fs: metadata exceeds 16 KiB")
	}
	name := ".tmp-" + rand.Text()
	f, err := b.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	defer b.root.Remove(name)
	if _, err = f.Write(append(header, '\n')); err != nil {
		return err
	}
	n, err := io.Copy(f, contextReader{ctx, body})
	if err != nil {
		return err
	}
	if n != object.Size {
		return fmt.Errorf("fs: body size %d differs from declared %d", n, object.Size)
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = b.root.Rename(name, filename(object.Key)); err != nil {
		return err
	}
	return b.syncDirectory()
}

// Directory sync failure occurs after publication and must retain the quota.
func (b *Backend) syncDirectory() error {
	dir, err := b.root.Open(".")
	if err != nil {
		return errors.Join(storage.ErrOutcomeUnknown, err)
	}
	defer dir.Close()
	if err = dir.Sync(); err != nil {
		return errors.Join(storage.ErrOutcomeUnknown, err)
	}
	return nil
}

type objectBody struct {
	io.Reader
	file *os.File
}

func (b *objectBody) Close() error { return b.file.Close() }

func (b *Backend) open(ctx context.Context, name string) (storage.Object, io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return storage.Object{}, nil, err
	}
	info, err := b.root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return storage.Object{}, nil, storage.ErrNotFound
	}
	if err != nil {
		return storage.Object{}, nil, err
	}
	if !info.Mode().IsRegular() {
		return storage.Object{}, nil, fmt.Errorf("fs: non-regular object %q", name)
	}
	f, err := b.root.Open(name)
	if errors.Is(err, os.ErrNotExist) {
		return storage.Object{}, nil, storage.ErrNotFound
	}
	if err != nil {
		return storage.Object{}, nil, err
	}
	fail := func(err error) (storage.Object, io.ReadCloser, error) { f.Close(); return storage.Object{}, nil, err }
	// Bound corrupt metadata rather than allocating until an arbitrary newline.
	reader := bufio.NewReaderSize(f, 16*1024)
	header, err := reader.ReadSlice('\n')
	if err != nil {
		return fail(fmt.Errorf("fs: invalid metadata: %w", err))
	}
	var object storage.Object
	if err = json.Unmarshal(header, &object); err != nil {
		return fail(err)
	}
	info, err = f.Stat()
	if err != nil {
		return fail(err)
	}
	if err = storage.ValidateKey(object.Key); err != nil {
		return fail(err)
	}
	if filename(object.Key) != name || object.Size < 0 || object.Size != info.Size()-int64(len(header)) {
		return fail(fmt.Errorf("fs: inconsistent object %q", name))
	}
	return object, &objectBody{Reader: contextReader{ctx, io.LimitReader(reader, object.Size)}, file: f}, nil
}

func (b *Backend) Get(ctx context.Context, key string) (storage.Object, io.ReadCloser, error) {
	if err := storage.ValidateKey(key); err != nil {
		return storage.Object{}, nil, err
	}
	if err := b.limiter.Allow(ctx, Operations); err != nil {
		return storage.Object{}, nil, err
	}
	object, body, err := b.open(ctx, filename(key))
	if err == nil && object.Key != key {
		body.Close()
		return storage.Object{}, nil, fmt.Errorf("fs: key collision")
	}
	return object, body, err
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	if err := storage.ValidateKey(key); err != nil {
		return err
	}
	if err := b.limiter.Allow(ctx, Operations); err != nil {
		return err
	}
	if err := b.root.Remove(filename(key)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	return b.syncDirectory()
}

func (b *Backend) List(ctx context.Context) ([]storage.Object, error) {
	if err := b.limiter.Allow(ctx, Operations); err != nil {
		return nil, err
	}
	dir, err := b.root.Open(".")
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	entries, err := dir.ReadDir(-1)
	if err != nil {
		return nil, err
	}
	objects := make([]storage.Object, 0)
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "obj-") {
			continue
		}
		object, body, err := b.open(ctx, entry.Name())
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if err = body.Close(); err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	return objects, nil
}

var _ storage.Backend = (*Backend)(nil)
