package storage

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"sync"

	"github.com/google/uuid"
)

type errStr string

func (e errStr) Error() string {
	return string(e)
}

type hashingLimitReader struct {
	r       io.Reader
	h       hash.Hash
	n       int64
	limit   int64
	consume []func(int64) error
}

type mutableState struct {
	maxSize  int64
	size     int64
	inflight int64
	objects  map[string]int64
	mu       sync.Mutex
}

type putReservation struct {
	state        *mutableState
	key          string
	previousSize int64
	reserved     int64
	observed     int64
}

const (
	// ErrMaxBytesReached indicates that reading more bytes would exceed the
	// configured storage limit.
	ErrMaxBytesReached = errStr("maxBytes reached")
	// ErrObjectNotFound indicates that an object does not exist.
	ErrObjectNotFound = errStr("object not found")
)

func newMutableState(maxSize int64) *mutableState {
	return &mutableState{
		maxSize: maxSize,
		objects: make(map[string]int64),
	}
}

func randomUUID() (string, error) {
	id, err := uuid.NewRandomFromReader(rand.Reader)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func newHashingLimitReader(r io.Reader, limit int64, consume ...func(int64) error) *hashingLimitReader {
	h := sha256.New()
	return &hashingLimitReader{
		r:       r,
		h:       h,
		limit:   limit,
		consume: consume,
	}
}

func (hlr *hashingLimitReader) Read(p []byte) (int, error) {
	n, err := hlr.r.Read(p)
	if n <= 0 {
		return n, err
	}

	allowed := int64(n)
	if hlr.limit >= 0 {
		remaining := hlr.limit - hlr.n
		if remaining <= 0 {
			return 0, ErrMaxBytesReached
		}
		if allowed > remaining {
			allowed = remaining
		}
	}

	for _, f := range hlr.consume {
		if err := f(allowed); err != nil {
			return 0, err
		}
	}

	if _, err := hlr.h.Write(p[:allowed]); err != nil {
		return int(allowed), err
	}

	hlr.n += allowed

	if allowed < int64(n) {
		return int(allowed), ErrMaxBytesReached
	}

	return int(allowed), err
}

func (hlr *hashingLimitReader) Size() int64 {
	return hlr.n
}

func (hlr *hashingLimitReader) SHA256() string {
	return hex.EncodeToString(hlr.h.Sum(nil))
}

func (s *mutableState) load(objects map[string]int64, size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.objects = objects
	s.size = size
	s.inflight = 0
}

func (s *mutableState) startPut(key string, knownSize int64) (*putReservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reservation := &putReservation{
		state:        s,
		key:          key,
		previousSize: s.objects[key],
	}

	if knownSize > 0 {
		delta := knownSize - reservation.previousSize
		if delta > 0 {
			if s.size+s.inflight+delta > s.maxSize {
				return nil, errStr("max bytes reached")
			}
			s.inflight += delta
			reservation.reserved = delta
		}
	}

	return reservation, nil
}

func (s *mutableState) abortPut(reservation *putReservation) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.inflight -= reservation.reserved
	if s.inflight < 0 {
		s.inflight = 0
	}
}

func (s *mutableState) finishPut(reservation *putReservation, finalSize int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.inflight -= reservation.reserved
	if s.inflight < 0 {
		s.inflight = 0
	}

	s.size += finalSize - reservation.previousSize
	if s.size < 0 {
		s.size = 0
	}

	s.objects[reservation.key] = finalSize
}

func (r *putReservation) consume(n int64) error {
	r.observed += n

	required := r.observed - r.previousSize
	if required <= r.reserved {
		return nil
	}

	additional := required - r.reserved

	r.state.mu.Lock()
	defer r.state.mu.Unlock()

	if r.state.size+r.state.inflight+additional > r.state.maxSize {
		return errStr("max bytes reached")
	}

	r.state.inflight += additional
	r.reserved += additional

	return nil
}

func (s *mutableState) removeObject(key string, fallbackSize int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	size, ok := s.objects[key]
	if !ok {
		size = fallbackSize
	} else {
		delete(s.objects, key)
	}

	s.size -= size
	if s.size < 0 {
		s.size = 0
	}
}
