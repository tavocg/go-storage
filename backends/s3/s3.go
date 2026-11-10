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
	storage "github.com/tavocg/go-storage/v2"
)

// Client is the AWS SDK surface used by Backend. The application configures
// credentials, endpoint, region, path-style addressing, retry and HTTP policies.
type Client interface {
	PutObject(context.Context, *awss3.PutObjectInput, ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
	GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	DeleteObject(context.Context, *awss3.DeleteObjectInput, ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error)
	ListObjectsV2(context.Context, *awss3.ListObjectsV2Input, ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
}

type Backend struct {
	client Client
	bucket string
}

func New(client Client, bucket string) (*Backend, error) {
	if client == nil || strings.TrimSpace(bucket) == "" {
		return nil, fmt.Errorf("s3: client and bucket are required")
	}
	return &Backend{client: client, bucket: bucket}, nil
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
	_, err = b.client.PutObject(ctx, input)
	// SDK retries can obscure a previous successful attempt with a lost response.
	// Conservatively retain reservations on every error after invoking the SDK.
	if err != nil {
		return errors.Join(storage.ErrOutcomeUnknown, err)
	}
	return nil
}

func (b *Backend) Get(ctx context.Context, key string) (storage.Object, io.ReadCloser, error) {
	if err := storage.ValidateKey(key); err != nil {
		return storage.Object{}, nil, err
	}
	if err := ctx.Err(); err != nil {
		return storage.Object{}, nil, err
	}
	output, err := b.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key)})
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
	_, err := b.client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key)})
	if err != nil {
		return errors.Join(storage.ErrOutcomeUnknown, err)
	}
	return nil
}

func (b *Backend) List(ctx context.Context) ([]storage.Object, error) {
	paginator := awss3.NewListObjectsV2Paginator(b.client, &awss3.ListObjectsV2Input{Bucket: aws.String(b.bucket)})
	objects := make([]storage.Object, 0)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
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
