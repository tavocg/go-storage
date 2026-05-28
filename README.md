# go-storage

Golang S3-backed object storage wrapper with byte accounting.

## Usage

```go
package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/tavocg/go-storage"
)

func example(ctx context.Context, s *storage.Storage) error {
	head, err := s.Put(
		ctx,
		strings.NewReader("hello world"),
		storage.WithKey("greeting.txt"),
		storage.WithSizeLimit(int64(len("hello world"))),
	)
	if err != nil {
		return err
	}

	body, err := s.Get(ctx, head)
	if err != nil {
		return err
	}
	defer body.Close()

	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}

	fmt.Println(string(data))
	fmt.Println(s.Exists(head.Key))
	fmt.Println(s.List())

	if err := s.Delete(ctx, head); err != nil {
		return err
	}

	return nil
}
```

## Custom Endpoints

For S3-compatible providers such as Cloudflare R2, use the S3 backend package
and pass a custom endpoint:

```go
package main

import (
	"context"

	s3backend "github.com/tavocg/go-storage/backends/s3"
)

func newStore(ctx context.Context) error {
	_, err := s3backend.New(
		ctx,
		s3backend.WithBucket("personal"),
		s3backend.WithRegion("auto"),
		s3backend.WithEndpoint("https://11bf4a9e76b5bde4ca62baa852624281.r2.cloudflarestorage.com"),
		s3backend.WithPublicEndpointURL("https://pub-11bf4a9e76b5bde4ca62baa852624281.r2.dev"),
	)
	return err
}
```

When `WithPublicEndpointURL` is set, `GetBody` downloads objects directly from
that public base URL; otherwise it uses the configured S3 backend.
