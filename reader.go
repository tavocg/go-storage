package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
)

type hashingLimitReader struct {
	r       io.Reader
	h       hash.Hash
	n       int64
	limit   int64
	consume []func(int64) error
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
