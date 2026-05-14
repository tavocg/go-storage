// Package storage <SHORT DESCRIPTION HERE>
package storage

import (
	"context"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Storage struct {
	backend Backend
	bucket  string
	maxSize int64

	size     int64
	inflight int64
	mu       sync.RWMutex
}

type PutOption func(*ObjectHead)

func WithKey(key string) PutOption {
	return func(o *ObjectHead) {
		o.Key = key
	}
}

func WithSize(size int64) PutOption {
	return func(o *ObjectHead) {
		o.Size = size
	}
}

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
		// Limited by the object size, faster
		if err := s.reserveBytes(oh.Size); err != nil {
			return nil, err
		}
		reserved = oh.Size
		hlr = newHashingLimitReader(body, oh.Size)
	} else {
		// Unlimited, until reserveBytes callback fails, slower
		hlr = newHashingLimitReader(body, -1, s.reserveBytes)
	}

	// Prepare s3 input
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
		s.releaseBytes(reserved - hlr.Size())
	}
	s.commitBytes(hlr.Size())
	oh.Size = hlr.Size()
	oh.SHA256 = hlr.SHA256()

	return &oh, nil
}

func (s *Storage) Get(ctx context.Context, head ObjectHead) (io.Reader, error) {
	obj, err := s.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(head.Key),
	})
	if err != nil {
		return nil, err
	}
	return obj.Body, err
}

func (s *Storage) Delete(ctx context.Context, head ObjectHead) error {
	if _, err := s.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(head.Key),
	}); err != nil {
		if !isNotFound(err) {
			return err
		}
		return nil
	}

	if head.Size > 0 {
		s.removeBytes(head.Size)
	}

	return nil
}
