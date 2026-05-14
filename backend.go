package storage

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type Backend interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

func isNotFound(err error) bool {
	if errors.Is(err, ErrObjectNotFound) {
		return true
	}

	if _, ok := errors.AsType[*types.NoSuchKey](err); ok {
		return true
	}

	if _, ok := errors.AsType[*types.NotFound](err); ok {
		return true
	}

	apiErr, ok := errors.AsType[smithy.APIError](err)
	return ok && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound")
}
