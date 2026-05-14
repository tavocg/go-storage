package storage

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"

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

const (
	// ErrMaxBytesReached indicates that reading more bytes would exceed the
	// configured storage limit.
	ErrMaxBytesReached = errStr("maxBytes reached")
	// ErrObjectNotFound indicates that an object does not exist.
	ErrObjectNotFound = errStr("object not found")
)

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

func (s *Storage) reserveBytes(n int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.inflight+s.size+n > s.maxSize {
		return errStr("max bytes reached")
	}

	s.inflight += n
	return nil
}

func (s *Storage) releaseBytes(n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflight -= n
}

func (s *Storage) commitBytes(n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflight -= n
	s.size += n
}

func (s *Storage) removeBytes(n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.size -= n
	if s.size < 0 {
		s.size = 0
	}
}
