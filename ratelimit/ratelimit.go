// Package ratelimit admits backend operations against category-specific,
// strict sliding-window budgets. It is independent of byte reservations.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Category string

// Limit bounds admitted attempts in every Interval. A zero limit disables it.
type Limit struct {
	Operations int
	Interval   time.Duration
}

type Options struct {
	// Scope identifies the shared budget, such as an R2 account, not a bucket.
	Scope  string
	Limits map[Category]Limit
	// Nil selects a private in-memory coordinator for this backend instance.
	Coordinator Coordinator
}

var (
	ErrLimited           = errors.New("ratelimit: operation budget exhausted")
	ErrCoordination      = errors.New("ratelimit: coordination failed")
	ErrConflictingLimits = errors.New("ratelimit: conflicting limits")
)

// Error describes the rejected budget and earliest retry delay.
type Error struct {
	Scope      string
	Category   Category
	RetryAfter time.Duration
}

func (e *Error) Error() string {
	return fmt.Sprintf("%v for %s/%s (retry after %s)", ErrLimited, e.Scope, e.Category, e.RetryAfter)
}
func (e *Error) Unwrap() error { return ErrLimited }

// Coordinator atomically admits and charges attempts across backend instances.
// Implementations must persist history across process restarts when needed,
// reject conflicting limits for a shared scope/category, honor context, and
// reject immediately with ErrLimited rather than waiting for capacity.
// Charges are never refunded: a failed request may still have been billed.
// An error must never allow the caller to send the request.
type Coordinator interface {
	Allow(ctx context.Context, scope string, category Category, limit Limit) error
}

// Limiter owns an immutable copy of the configuration. Its zero value permits
// all categories. Share Coordinator and Scope to share budgets across backends.
type Limiter struct {
	scope       string
	limits      map[Category]Limit
	coordinator Coordinator
}

func New(options Options) (*Limiter, error) {
	if len(options.Limits) > 0 && strings.TrimSpace(options.Scope) == "" {
		return nil, fmt.Errorf("ratelimit: scope is required")
	}
	limits := make(map[Category]Limit, len(options.Limits))
	for category, limit := range options.Limits {
		if strings.TrimSpace(string(category)) == "" {
			return nil, fmt.Errorf("ratelimit: category is required")
		}
		if err := validate(limit); err != nil {
			return nil, err
		}
		limits[category] = limit
	}
	c := options.Coordinator
	if c == nil {
		c = &MemoryCoordinator{}
	}
	return &Limiter{scope: options.Scope, limits: limits, coordinator: c}, nil
}
func validate(limit Limit) error {
	if limit.Operations < 0 || limit.Interval < 0 || (limit.Operations == 0) != (limit.Interval == 0) {
		return fmt.Errorf("ratelimit: invalid limit")
	}
	return nil
}

// Allow admits one attempt in category. Missing and zero categories have no
// limit. Backend implementations call it immediately before dispatching work.
func (l *Limiter) Allow(ctx context.Context, category Category) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	limit, configured := l.limits[category]
	if !configured {
		return nil
	}
	err := l.coordinator.Allow(ctx, l.scope, category, limit)
	if err == nil || errors.Is(err, ErrLimited) || errors.Is(err, ErrConflictingLimits) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return errors.Join(ErrCoordination, err)
}
