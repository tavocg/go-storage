package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
	"unicode/utf8"
)

// Options configure one namespace. Scope must uniquely identify the backend
// namespace, including endpoint/account when necessary. Equal scopes must not
// refer to different namespaces. Limits must match across coordinated users.
type Options struct {
	Scope       string
	Limits      Limits
	Coordinator Coordinator
	// TempDir holds staged uploads; empty uses the OS temporary directory.
	TempDir string
	// FinalizeTimeout bounds cleanup independent of caller cancellation.
	// Zero selects five seconds.
	FinalizeTimeout time.Duration
}

// Storage is safe for concurrent use. Without a Coordinator, New creates a
// private MemoryCoordinator and scans existing objects before returning.
// Only calls through this Storage are then covered by its quota guarantees.
type Storage struct {
	backend     Backend
	options     Options
	coordinator Coordinator
}

func New(ctx context.Context, backend Backend, options Options) (*Storage, error) {
	if backend == nil || strings.TrimSpace(options.Scope) == "" {
		return nil, fmt.Errorf("storage: backend and scope are required")
	}
	if err := validateLimits(options.Limits); err != nil {
		return nil, err
	}
	if options.FinalizeTimeout < 0 {
		return nil, fmt.Errorf("storage: invalid finalization timeout")
	}
	if options.FinalizeTimeout == 0 {
		options.FinalizeTimeout = 5 * time.Second
	}
	c := options.Coordinator
	if c == nil {
		c = NewMemoryCoordinator()
	}
	s := &Storage{backend: backend, options: options, coordinator: c}
	if err := c.Initialize(ctx, options.Scope, options.Limits, func(ctx context.Context) (map[string]int64, error) {
		objects, err := backend.List(ctx)
		if err != nil {
			return nil, err
		}
		snapshot := make(map[string]int64, len(objects))
		var total int64
		for _, o := range objects {
			if err := ValidateKey(o.Key); err != nil {
				return nil, err
			}
			if o.Size < 0 || o.Size > math.MaxInt64-total {
				return nil, fmt.Errorf("storage: invalid backend size")
			}
			if _, exists := snapshot[o.Key]; exists {
				return nil, fmt.Errorf("storage: duplicate backend key %q", o.Key)
			}
			snapshot[o.Key] = o.Size
			total += o.Size
		}
		return snapshot, nil
	}); err != nil {
		return nil, errors.Join(ErrCoordination, err)
	}
	return s, nil
}

// ValidateKey checks the portable key envelope used by Storage. Backends may
// impose further service-specific restrictions. Keys are not filesystem paths.
func ValidateKey(key string) error {
	if key == "" || len(key) > 1024 || !utf8.ValidString(key) || strings.ContainsRune(key, 0) {
		return ErrInvalidKey
	}
	return nil
}

type putOptions struct {
	contentType string
	limit       int64
}
type PutOption func(*putOptions)

func WithContentType(value string) PutOption { return func(o *putOptions) { o.contentType = value } }

// WithSizeLimit bounds the body size. Oversized bodies fail without changing
// the stored object; they are never silently truncated. Zero permits empty
// bodies only. Without this option the total quota bounds staging, if enabled.
func WithSizeLimit(bytes int64) PutOption { return func(o *putOptions) { o.limit = bytes } }

// Put stages and hashes body in a temporary file before reserving its exact
// size. This supports unknown-size readers and seekable SDK retries. Staging
// uses disk, not memory; temporary bytes are not part of the logical quota.
// Cancellation cannot interrupt a body.Read that itself blocks indefinitely.
func (s *Storage) Put(ctx context.Context, key string, body io.Reader, opts ...PutOption) (Object, error) {
	if err := ValidateKey(key); err != nil {
		return Object{}, err
	}
	if body == nil {
		return Object{}, fmt.Errorf("storage: body is required")
	}
	options := putOptions{limit: -1}
	for _, opt := range opts {
		if opt == nil {
			return Object{}, fmt.Errorf("storage: nil put option")
		}
		opt(&options)
	}
	if options.limit < -1 {
		return Object{}, fmt.Errorf("storage: invalid upload limit")
	}
	if err := s.allow(ctx, OperationPut); err != nil {
		return Object{}, err
	}
	file, object, err := s.stage(ctx, key, body, options)
	if err != nil {
		return Object{}, err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	m := Mutation{Scope: s.options.Scope, ID: rand.Text(), Key: key, Operation: OperationPut, Size: object.Size}
	r, err := s.coordinator.Reserve(ctx, m)
	if err != nil {
		return Object{}, errors.Join(ErrCoordination, err)
	}
	if r == nil {
		return Object{}, fmt.Errorf("%w: nil reservation", ErrCoordination)
	}
	err = s.backend.Put(ctx, object, file)
	if err := s.finish(ctx, m, r, err); err != nil {
		return Object{}, err
	}
	return object, nil
}

// Get opens the current backend object. The caller must close the body.
func (s *Storage) Get(ctx context.Context, key string) (Object, io.ReadCloser, error) {
	if err := ValidateKey(key); err != nil {
		return Object{}, nil, err
	}
	if err := s.allow(ctx, OperationGet); err != nil {
		return Object{}, nil, err
	}
	return s.backend.Get(ctx, key)
}

// List returns the backend's current objects, not a cached coordinator view.
// One call counts as one operation even if the backend uses multiple pages.
func (s *Storage) List(ctx context.Context) ([]Object, error) {
	if err := s.allow(ctx, OperationList); err != nil {
		return nil, err
	}
	return s.backend.List(ctx)
}

// Delete is idempotent, including for accounting. The backend is still called
// for missing keys, and each admitted call counts toward the operation limit.
func (s *Storage) Delete(ctx context.Context, key string) error {
	if err := ValidateKey(key); err != nil {
		return err
	}
	if err := s.allow(ctx, OperationDelete); err != nil {
		return err
	}
	m := Mutation{Scope: s.options.Scope, ID: rand.Text(), Key: key, Operation: OperationDelete}
	r, err := s.coordinator.Reserve(ctx, m)
	if err != nil {
		return errors.Join(ErrCoordination, err)
	}
	if r == nil {
		return fmt.Errorf("%w: nil reservation", ErrCoordination)
	}
	return s.finish(ctx, m, r, s.backend.Delete(ctx, key))
}

func (s *Storage) allow(ctx context.Context, op Operation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.coordinator.Allow(ctx, s.options.Scope, op); err != nil {
		return errors.Join(ErrCoordination, err)
	}
	return nil
}

func (s *Storage) finish(ctx context.Context, m Mutation, r Reservation, backendErr error) error {
	if errors.Is(backendErr, ErrOutcomeUnknown) {
		return &PendingError{Scope: m.Scope, Key: m.Key, ID: m.ID, Reservation: r, Cause: backendErr}
	}
	outcome := Applied
	if backendErr != nil {
		outcome = Unchanged
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.options.FinalizeTimeout)
	defer cancel()
	if err := r.Complete(cleanup, outcome); err != nil {
		return &PendingError{Scope: m.Scope, Key: m.Key, ID: m.ID, Reservation: r, Suggested: outcome, Cause: errors.Join(ErrCoordination, backendErr, err)}
	}
	return backendErr
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func (s *Storage) stage(ctx context.Context, key string, body io.Reader, opts putOptions) (*os.File, Object, error) {
	f, err := os.CreateTemp(s.options.TempDir, "go-storage-*")
	if err != nil {
		return nil, Object{}, err
	}
	success := false
	defer func() {
		if !success {
			f.Close()
			os.Remove(f.Name())
		}
	}()
	limit := int64(math.MaxInt64)
	limitErr := ErrObjectTooLarge
	if opts.limit >= 0 {
		limit = opts.limit
	}
	if maxBytes := s.options.Limits.MaxBytes; maxBytes > 0 && maxBytes < limit {
		limit = maxBytes
		limitErr = ErrQuotaExceeded
	}
	source := contextReader{ctx, body}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(source, limit))
	if err != nil {
		return nil, Object{}, err
	}
	if n == limit {
		var extra [1]byte
		if count, probeErr := io.ReadFull(source, extra[:]); count > 0 {
			return nil, Object{}, limitErr
		} else if probeErr != io.EOF {
			return nil, Object{}, probeErr
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, Object{}, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, Object{}, err
	}
	success = true
	return f, Object{Key: key, Size: n, ContentType: opts.contentType, SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}
