# go-storage

Golang S3-backed object storage wrapper with byte accounting.

## Usage

This package currently exposes the storage operations, but not a constructor.
That means your code needs to obtain a `*storage.Storage` from inside this
module or from a constructor you add later.

Minimal usage looks like this:

```go
package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	storage "github.com/tavocg/go-storage"
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

	if err := s.Delete(ctx, head); err != nil {
		return err
	}

	return nil
}
```

## Notes

- `WithSizeLimit` sets the maximum number of bytes `Put` will read from the input.
- The returned `ObjectHead.Size` is the number of bytes actually stored.
- `Get` returns an `io.ReadCloser`; the caller must close it.
