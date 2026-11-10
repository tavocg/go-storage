// Package storage provides object storage with byte quotas and operation limits.
package storage

import (
	"context"
	"io"
)

// Backend stores objects in one namespace and must be safe for concurrent use.
// Put atomically replaces an object: readers see the old or complete new body.
// Delete is idempotent. A mutation error guarantees the object is unchanged,
// unless it wraps ErrOutcomeUnknown (for example a lost network response).
// Put must store exactly object.Size bytes or fail, and must not retain body.
// List excludes temporary uploads and historical versions.
type Backend interface {
	Put(ctx context.Context, object Object, body io.ReadSeeker) error
	Get(ctx context.Context, key string) (Object, io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context) ([]Object, error)
}
