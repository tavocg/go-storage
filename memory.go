package storage

import (
	"context"
	"fmt"
	"math"
	"sync"
)

// MemoryCoordinator coordinates instances in this process only. Share one
// explicitly when constructing multiple Storage instances for the same scope.
// Its zero value is ready to use. It does not persist across restarts.
type MemoryCoordinator struct {
	mu           sync.Mutex
	scopes       map[string]*memoryScope
	initializing map[string]chan struct{}
}
type memoryScope struct {
	limits   Limits
	objects  map[string]int64
	used     int64
	reserved int64
	pending  map[string]*memoryReservation
	ids      map[string]bool
}
type memoryReservation struct {
	coordinator *MemoryCoordinator
	scope       *memoryScope
	mutation    Mutation
	previous    int64
	delta       int64
	done        Outcome
}

func NewMemoryCoordinator() *MemoryCoordinator { return &MemoryCoordinator{} }

func validateLimits(l Limits) error {
	if l.MaxBytes < 0 {
		return fmt.Errorf("storage: invalid byte limit")
	}
	return nil
}

func (c *MemoryCoordinator) Initialize(ctx context.Context, scope string, limits Limits, scan func(context.Context) (map[string]int64, error)) error {
	if scope == "" || scan == nil {
		return fmt.Errorf("storage: scope and scanner are required")
	}
	if err := validateLimits(limits); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.mu.Lock()
		if s := c.scopes[scope]; s != nil {
			c.mu.Unlock()
			if s.limits != limits {
				return ErrConflictingLimits
			}
			return nil
		}
		if waiting := c.initializing[scope]; waiting != nil {
			c.mu.Unlock()
			select {
			case <-waiting:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if c.initializing == nil {
			c.initializing = make(map[string]chan struct{})
		}
		ready := make(chan struct{})
		c.initializing[scope] = ready
		c.mu.Unlock()

		s, err := scanScope(ctx, limits, scan)
		c.mu.Lock()
		if err == nil {
			if c.scopes == nil {
				c.scopes = make(map[string]*memoryScope)
			}
			c.scopes[scope] = s
		}
		delete(c.initializing, scope)
		close(ready)
		c.mu.Unlock()
		return err
	}
}

func scanScope(ctx context.Context, limits Limits, scan func(context.Context) (map[string]int64, error)) (*memoryScope, error) {
	objects, err := scan(ctx)
	if err != nil {
		return nil, err
	}
	s := &memoryScope{limits: limits, objects: make(map[string]int64), pending: make(map[string]*memoryReservation), ids: make(map[string]bool)}
	for key, size := range objects {
		if err := ValidateKey(key); err != nil {
			return nil, err
		}
		if size < 0 || size > math.MaxInt64-s.used {
			return nil, fmt.Errorf("storage: invalid or overflowing initial size")
		}
		s.objects[key] = size
		s.used += size
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s, nil
}

func (c *MemoryCoordinator) Reserve(ctx context.Context, m Mutation) (Reservation, error) {
	if err := ValidateKey(m.Key); err != nil {
		return nil, err
	}
	if m.ID == "" || m.Size < 0 || (m.Operation != OperationPut && m.Operation != OperationDelete) ||
		(m.Operation == OperationDelete && m.Size != 0) {
		return nil, ErrInvalidReservation
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s := c.scopes[m.Scope]
	if s == nil {
		return nil, fmt.Errorf("storage: uninitialized scope")
	}
	if s.pending[m.Key] != nil {
		return nil, ErrObjectBusy
	}
	if s.ids[m.ID] {
		return nil, ErrInvalidReservation
	}
	previous := s.objects[m.Key]
	delta := max(m.Size-previous, 0)
	// Apply the overflow guard even without a configured quota.
	ceiling := s.limits.MaxBytes
	if ceiling == 0 {
		ceiling = math.MaxInt64
	}
	if delta > 0 && (s.used > ceiling || s.reserved > ceiling-s.used || delta > ceiling-s.used-s.reserved) {
		return nil, ErrQuotaExceeded
	}
	r := &memoryReservation{coordinator: c, scope: s, mutation: m, previous: previous, delta: delta}
	s.reserved += delta
	s.pending[m.Key] = r
	s.ids[m.ID] = true
	return r, nil
}

func (r *memoryReservation) Complete(ctx context.Context, outcome Outcome) error {
	if outcome != Applied && outcome != Unchanged {
		return ErrInvalidReservation
	}
	c := r.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.done != 0 {
		if r.done != outcome {
			return ErrInvalidReservation
		}
		return nil
	}
	s, m := r.scope, r.mutation
	if outcome == Applied {
		s.used -= r.previous
		if m.Operation == OperationPut {
			s.objects[m.Key] = m.Size
			s.used += m.Size
		} else {
			delete(s.objects, m.Key)
		}
	}
	s.reserved -= r.delta
	delete(s.pending, m.Key)
	delete(s.ids, m.ID)
	r.done = outcome
	return nil
}
