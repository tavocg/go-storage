package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type budgetKey struct {
	scope    string
	category Category
}
type budget struct {
	limit Limit
	calls []time.Time
	head  int
}

// MemoryCoordinator is safe for concurrent use; its zero value is usable.
// History is local to this process and is lost on restart. Share one explicitly
// across backend instances to prevent each instance receiving its own budget.
type MemoryCoordinator struct {
	mu      sync.Mutex
	budgets map[budgetKey]*budget
	now     func() time.Time
}

func (c *MemoryCoordinator) Allow(ctx context.Context, scope string, category Category, limit Limit) error {
	if scope == "" || category == "" {
		return fmt.Errorf("ratelimit: scope and category are required")
	}
	if err := validate(limit); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	key := budgetKey{scope, category}
	b := c.budgets[key]
	if b == nil {
		if c.budgets == nil {
			c.budgets = make(map[budgetKey]*budget)
		}
		b = &budget{limit: limit}
		c.budgets[key] = b
	} else if b.limit != limit {
		return ErrConflictingLimits
	}
	if limit.Operations == 0 {
		return nil
	}
	now := time.Now()
	if c.now != nil {
		now = c.now()
	}
	cutoff := now.Add(-limit.Interval)
	for b.head < len(b.calls) && !b.calls[b.head].After(cutoff) {
		b.head++
	}
	// Compact occasionally, rather than copying a large window on every
	// expiration. Both admission and expiry are amortized constant time.
	if b.head == len(b.calls) {
		b.calls = b.calls[:0]
		b.head = 0
	} else if b.head >= 1024 && b.head >= len(b.calls)/2 {
		copy(b.calls, b.calls[b.head:])
		b.calls = b.calls[:len(b.calls)-b.head]
		b.head = 0
	}
	if len(b.calls)-b.head >= limit.Operations {
		return &Error{Scope: scope, Category: category, RetryAfter: b.calls[b.head].Add(limit.Interval).Sub(now)}
	}
	b.calls = append(b.calls, now)
	return nil
}
