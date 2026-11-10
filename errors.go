package storage

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrQuotaExceeded      = errors.New("storage: byte quota exceeded")
	ErrRateLimited        = errors.New("storage: operation rate exceeded")
	ErrObjectBusy         = errors.New("storage: object has a pending mutation")
	ErrNotFound           = errors.New("storage: object not found")
	ErrInvalidKey         = errors.New("storage: invalid object key")
	ErrObjectTooLarge     = errors.New("storage: object exceeds upload limit")
	ErrOutcomeUnknown     = errors.New("storage: mutation outcome unknown")
	ErrCoordination       = errors.New("storage: coordination failed")
	ErrConflictingLimits  = errors.New("storage: conflicting limits for scope")
	ErrInvalidReservation = errors.New("storage: invalid reservation")
)

// RateLimitError gives the delay until the oldest operation leaves the window.
type RateLimitError struct{ RetryAfter time.Duration }

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("%v (retry after %s)", ErrRateLimited, e.RetryAfter)
}
func (e *RateLimitError) Unwrap() error { return ErrRateLimited }

// PendingError means a mutation needs recovery. Its reservation remains held
// unless a finalization retry already completed it. After fencing the original
// writer and establishing the outcome, call Reservation.Complete with Applied
// or Unchanged. Never release it merely because a timeout expired.
// Suggested is the outcome to retry if only finalization failed; zero means the
// backend outcome is unknown. ID identifies a durable coordinator reservation.
type PendingError struct {
	Scope       string
	Key         string
	ID          string
	Reservation Reservation
	Suggested   Outcome
	Cause       error
}

func (e *PendingError) Error() string {
	return fmt.Sprintf("storage: pending mutation %s for %q: %v", e.ID, e.Key, e.Cause)
}
func (e *PendingError) Unwrap() error { return e.Cause }
