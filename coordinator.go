package storage

import (
	"context"
	"time"
)

type Operation string

const (
	OperationPut    Operation = "put"
	OperationGet    Operation = "get"
	OperationDelete Operation = "delete"
	OperationList   Operation = "list"
)

// RateLimit is a strict sliding-window limit over logical API calls, not over
// backend HTTP requests. Zero Operations and Interval disable the rate limit.
type RateLimit struct {
	Operations int
	Interval   time.Duration
}

// Limits apply to an entire Scope. Zero MaxBytes disables the byte quota.
// Historical versions, multipart parts and staging files are excluded.
type Limits struct {
	MaxBytes int64
	Rate     RateLimit
}

// Mutation exclusively owns Key and reserves its positive size increase.
// Size is exact for Put and zero for Delete. ID is globally unique.
type Mutation struct {
	Scope     string
	ID        string
	Key       string
	Operation Operation
	Size      int64
}

type Outcome uint8

const (
	// Applied confirms that the reserved Put or Delete succeeded.
	Applied Outcome = iota + 1
	// Unchanged confirms that the mutation did not change the object.
	Unchanged
)

// Reservation retains capacity and exclusive mutation ownership of its key.
// Complete atomically updates accounting and releases ownership. It must be
// idempotent for the same outcome and reject a conflicting outcome. Errors
// must permit safe retries. Cancellation or expired leases must NEVER free
// capacity or allow an old writer to publish after capacity is reclaimed.
type Reservation interface {
	Complete(ctx context.Context, outcome Outcome) error
}

// Coordinator is implemented by applications needing coordination across
// processes. All instances of a namespace must share Scope, limits and
// coordinator. External mutations invalidate guarantees. Methods must be safe
// for concurrent use; errors must fail closed.
type Coordinator interface {
	// Initialize initializes a scope once, calling scan under exclusive
	// initialization ownership. Subsequent calls verify identical limits without
	// reloading state or discarding pending reservations. scan must not overlap
	// coordinated mutations of that scope during initialization.
	Initialize(ctx context.Context, scope string, limits Limits, scan func(context.Context) (map[string]int64, error)) error
	// Allow atomically charges one admitted operation. Charges are not refunded,
	// even if staging, quota checks or backend calls fail. Reject immediately
	// with ErrRateLimited instead of sleeping.
	Allow(ctx context.Context, scope string, operation Operation) error
	// Reserve atomically owns the key and checks committed bytes plus positive
	// reserved deltas against MaxBytes. Reject busy keys with ErrObjectBusy.
	// Do not release committed size until Complete confirms a successful change.
	Reserve(ctx context.Context, mutation Mutation) (Reservation, error)
}
