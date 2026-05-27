// Package storage provides a small S3-backed object storage wrapper with
// basic size accounting.
package storage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Storage stores objects in a single bucket and tracks total and in-flight
// byte usage against maxSize.
type Storage struct {
	backend           Backend
	bucket            string
	publicEndpointURL string
	httpClient        *http.Client
	state             *mutableState
}

// StorageOption configures Storage.
type StorageOption func(*Storage)

// New returns a Storage that uses backend to store objects in bucket and
// enforces maxSize as the total allowed byte usage.
func New(backend Backend, bucket string, maxSize int64, opts ...StorageOption) *Storage {
	store := &Storage{
		backend:    backend,
		bucket:     bucket,
		httpClient: http.DefaultClient,
		state:      newMutableState(maxSize),
	}

	for _, opt := range opts {
		opt(store)
	}

	return store
}

// WithPublicEndpointURL sets the base URL used by GetBody for public object
// downloads. When empty, GetBody uses the backend's GetObject method.
func WithPublicEndpointURL(rawURL string) StorageOption {
	return func(s *Storage) {
		s.publicEndpointURL = strings.TrimSpace(rawURL)
	}
}

// LoadState refreshes Storage's tracked byte usage from the backend bucket.
//
// Call LoadState after constructing Storage and before using it so in-memory
// accounting starts from the bucket's current size.
func (s *Storage) LoadState(ctx context.Context) error {
	objects := make(map[string]int64)
	var size int64

	p := s3.NewListObjectsV2Paginator(s.backend, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
	})

	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return err
		}

		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			objectSize := aws.ToInt64(obj.Size)
			objects[key] = objectSize
			size += objectSize
		}
	}

	s.state.load(objects, size)

	return nil
}

// List returns a copy of the tracked object sizes keyed by object name.
func (s *Storage) List() map[string]int64 {
	return s.state.list()
}

// Exists reports whether key is currently tracked in storage.
func (s *Storage) Exists(key string) bool {
	return s.state.exists(key)
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

// WithContentType sets the stored content type metadata for Put.
func WithContentType(contentType string) PutOption {
	return func(o *ObjectHead) {
		o.Type = contentType
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
	oh := ObjectHead{Size: -1}
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

	reservation, err := s.state.startPut(oh.Key, oh.Size)
	if err != nil {
		return nil, err
	}

	var hlr *hashingLimitReader
	if oh.Size >= 0 {
		// Size-limited uploads reserve once and use a bounded reader.
		hlr = newHashingLimitReader(body, oh.Size)
	} else {
		// Unknown-size uploads reserve incrementally only after they grow
		// beyond the size of the object they overwrite.
		hlr = newHashingLimitReader(body, -1, reservation.consume)
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(oh.Key),
		Body:   hlr,
	}
	if oh.Type != "" {
		input.ContentType = aws.String(oh.Type)
	}

	if _, err := s.backend.PutObject(ctx, input); err != nil {
		s.state.abortPut(reservation)
		return nil, err
	}

	s.state.finishPut(reservation, hlr.Size())
	oh.Size = hlr.Size()
	oh.SHA256 = hlr.SHA256()

	return &oh, nil
}

// GetBody opens an object body for reading.
//
// The caller must close the returned reader.
func (s *Storage) GetBody(ctx context.Context, head *ObjectHead) (io.ReadCloser, error) {
	if s.publicEndpointURL != "" {
		return s.getPublicBody(ctx, head.Key)
	}

	obj, err := s.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(head.Key),
	})
	if err != nil {
		return nil, err
	}
	return obj.Body, err
}

// Get opens an object body for reading.
//
// The caller must close the returned reader.
func (s *Storage) Get(ctx context.Context, head *ObjectHead) (io.ReadCloser, error) {
	return s.GetBody(ctx, head)
}

// Delete removes an object from the bucket and subtracts its tracked size from
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

	s.state.removeObject(head.Key, head.Size)

	return nil
}

func (s *Storage) getPublicBody(ctx context.Context, key string) (io.ReadCloser, error) {
	publicURL, err := publicObjectURL(s.publicEndpointURL, key)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, publicURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		_ = resp.Body.Close()
		return nil, ErrObjectNotFound
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("public get %q: unexpected status %s", key, resp.Status)
	}

	return resp.Body, nil
}

func publicObjectURL(baseURL, key string) (string, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse public endpoint URL: %w", err)
	}

	escapedKey := escapeObjectKey(key)
	base.Path = strings.TrimRight(base.Path, "/") + "/" + escapedKey

	return base.String(), nil
}

func escapeObjectKey(key string) string {
	parts := strings.Split(key, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}

	return strings.Join(parts, "/")
}
