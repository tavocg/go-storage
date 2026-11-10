// Package s3 implements storage.Backend for AWS S3 and compatible services.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	storage "github.com/tavocg/go-storage/v3"
	"github.com/tavocg/go-storage/v3/ratelimit"
)

type Backend struct {
	client  *awss3.Client
	bucket  string
	limiter *ratelimit.Limiter
}

func New(client *awss3.Client, bucket string, opts ...Option) (*Backend, error) {
	if client == nil || strings.TrimSpace(bucket) == "" {
		return nil, fmt.Errorf("s3: client and bucket are required")
	}
	limiter, err := newLimiter(opts)
	if err != nil {
		return nil, err
	}
	return &Backend{client: client, bucket: bucket, limiter: limiter}, nil
}

func (b *Backend) Put(ctx context.Context, object storage.Object, body io.ReadSeeker) error {
	if err := storage.ValidateKey(object.Key); err != nil {
		return err
	}
	if object.Size < 0 || body == nil {
		return fmt.Errorf("s3: invalid body or size")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	start, err := body.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	end, seekErr := body.Seek(0, io.SeekEnd)
	_, restoreErr := body.Seek(start, io.SeekStart)
	if err := errors.Join(seekErr, restoreErr); err != nil {
		return err
	}
	if end-start != object.Size {
		return fmt.Errorf("s3: body size differs from declared size")
	}
	input := &awss3.PutObjectInput{
		Bucket: aws.String(b.bucket), Key: aws.String(object.Key), Body: body,
		ContentLength: aws.Int64(object.Size),
		Metadata:      map[string]string{"go-storage-sha256": object.SHA256},
	}
	if object.ContentType != "" {
		input.ContentType = aws.String(object.ContentType)
	}
	tracker := &requestTracker{limiter: b.limiter, category: WriteRequests}
	_, err = b.client.PutObject(ctx, input, tracker.options)
	// A blocked first attempt is unchanged; any transmitted mutation can be
	// uncertain, including when a later retry was blocked by the rate limit.
	if err != nil && tracker.sent.Load() {
		return errors.Join(storage.ErrOutcomeUnknown, err)
	}
	return err
}

func (b *Backend) Get(ctx context.Context, key string) (storage.Object, io.ReadCloser, error) {
	if err := storage.ValidateKey(key); err != nil {
		return storage.Object{}, nil, err
	}
	if err := ctx.Err(); err != nil {
		return storage.Object{}, nil, err
	}
	tracker := &requestTracker{limiter: b.limiter, category: ReadRequests}
	output, err := b.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key)}, tracker.options)
	if err != nil {
		var api smithy.APIError
		if errors.As(err, &api) && (api.ErrorCode() == "NoSuchKey" || api.ErrorCode() == "NotFound") {
			return storage.Object{}, nil, storage.ErrNotFound
		}
		return storage.Object{}, nil, err
	}
	return storage.Object{Key: key, Size: aws.ToInt64(output.ContentLength), ContentType: aws.ToString(output.ContentType), SHA256: output.Metadata["go-storage-sha256"]}, output.Body, nil
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	if err := storage.ValidateKey(key); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	tracker := &requestTracker{limiter: b.limiter, category: FreeRequests}
	_, err := b.client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key)}, tracker.options)
	if err != nil && tracker.sent.Load() {
		return errors.Join(storage.ErrOutcomeUnknown, err)
	}
	return err
}

func (b *Backend) List(ctx context.Context) ([]storage.Object, error) {
	paginator := awss3.NewListObjectsV2Paginator(b.client, &awss3.ListObjectsV2Input{Bucket: aws.String(b.bucket)})
	objects := make([]storage.Object, 0)
	tracker := &requestTracker{limiter: b.limiter, category: WriteRequests}
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx, tracker.options)
		if err != nil {
			return nil, err
		}
		for _, o := range page.Contents {
			objects = append(objects, storage.Object{Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size)})
		}
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	return objects, nil
}

var _ storage.Backend = (*Backend)(nil)
