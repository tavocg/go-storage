// Package storage provides a small S3-backed object storage wrapper with
// basic size accounting.
package storage

import (
	"context"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Storage stores objects in a single bucket and tracks total and in-flight
// byte usage against maxSize.
type Storage struct {
	backend Backend
	bucket  string
	maxSize int64

	size     int64
	inflight int64
	mu       sync.RWMutex
}

// PutOption configures metadata for a Put request.
type PutOption func(*ObjectHead)

// WithKey sets the object key used by Put. If omitted, Put generates a random
// key.
func WithKey(key string) PutOption {
	return func(o *ObjectHead) {
		o.Key = key
	}
}

// WithSizeLimit sets the maximum number of bytes Put will read from body.
//
// Providing a size limit lets Put reserve capacity up front and use a bounded
// reader, which is slightly faster than reserving per read. The final stored
// object may be smaller if body ends before the limit.
func WithSizeLimit(size int64) PutOption {
	return func(o *ObjectHead) {
		o.Size = size
	}
}

// Put stores body in the configured bucket and returns the resulting object
// metadata.
//
// When WithSizeLimit is provided, Put can reserve storage up front and stream
// through a bounded reader, which is more efficient than reserving bytes on
// each read. The returned ObjectHead.Size is the number of bytes actually
// stored.
func (s *Storage) Put(ctx context.Context, body io.Reader, opts ...PutOption) (*ObjectHead, error) {
	oh := ObjectHead{}
	for _, opt := range opts {
		opt(&oh)
	}

	if oh.Key == "" {
		uuid, err := randomUUID()
		if err != nil {
			return nil, err
		}
		oh.Key = uuid
	}

	var hlr *hashingLimitReader
	reserved := int64(0)
	if oh.Size > 0 {
		// Known-size uploads reserve once and use a bounded reader.
		if err := s.reserveBytes(oh.Size); err != nil {
			return nil, err
		}
		reserved = oh.Size
		hlr = newHashingLimitReader(body, oh.Size)
	} else {
		// Unknown-size uploads reserve incrementally as bytes are read.
		hlr = newHashingLimitReader(body, -1, s.reserveBytes)
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(oh.Key),
		Body:   hlr,
	}
	if oh.Type != "" {
		input.ContentType = aws.String(oh.Type)
	}
	if oh.Size > 0 {
		input.ContentLength = aws.Int64(oh.Size)
	}

	if _, err := s.backend.PutObject(ctx, input); err != nil {
		if reserved > 0 {
			s.releaseBytes(reserved)
		} else {
			s.releaseBytes(hlr.Size())
		}
		return nil, err
	}

	if reserved > 0 {
		// Release any unused portion of the up-front reservation before
		// converting the consumed bytes into committed size.
		s.releaseBytes(reserved - hlr.Size())
	}
	s.commitBytes(hlr.Size())
	oh.Size = hlr.Size()
	oh.SHA256 = hlr.SHA256()

	return &oh, nil
}

// Get opens an object body for reading.
//
// The caller must close the returned reader.
func (s *Storage) Get(ctx context.Context, head *ObjectHead) (io.ReadCloser, error) {
	obj, err := s.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(head.Key),
	})
	if err != nil {
		return nil, err
	}
	return obj.Body, err
}

// Delete removes an object from the bucket.
//
// If head.Size is known, Delete also subtracts those bytes from the tracked
// storage usage.
func (s *Storage) Delete(ctx context.Context, head *ObjectHead) error {
	if _, err := s.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(head.Key),
	}); err != nil {
		if isNotFound(err) {
			return ErrObjectNotFound
		}
		return err
	}

	if head.Size > 0 {
		s.removeBytes(head.Size)
	}

	return nil
}
