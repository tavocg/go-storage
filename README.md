# go-storage

Go object-storage library with atomic byte reservations and operation limits.
Backends are independent of the AWS SDK: filesystem and S3-compatible backends
are included. There is no CLI and the repository is one Go module.

This is a breaking rewrite. The old constructors, `ObjectHead`, cached `List`
and command-line configuration are replaced by the API below. Requires Go 1.26.3+.

```sh
go get github.com/tavocg/go-storage/v2@v2.0.0
```

All backends are included in this module; their former separate module versions
are no longer used in v2.

## Filesystem

```go
package example

import (
    "context"
    "strings"
    "time"

    storage "github.com/tavocg/go-storage/v2"
    "github.com/tavocg/go-storage/v2/backends/fs"
)

func Example(ctx context.Context) error {
    backend, err := fs.New("./objects")
    if err != nil { return err }
    defer backend.Close()

    store, err := storage.New(ctx, backend, storage.Options{
        Scope: "local:uploads",
        Limits: storage.Limits{
            MaxBytes: 10 << 30,
            Rate: storage.RateLimit{Operations: 100, Interval: time.Minute},
        },
    })
    if err != nil { return err }

    object, err := store.Put(ctx, "greetings/hello.txt", strings.NewReader("hello"),
        storage.WithContentType("text/plain"),
        storage.WithSizeLimit(1024),
    )
    if err != nil { return err }
    return store.Delete(ctx, object.Key)
}
```

`Get(ctx, key)` returns `(Object, io.ReadCloser, error)`; close the returned body.
`List(ctx)` reads current backend objects. `Delete(ctx, key)` is idempotent.
`Put` replaces a key atomically. Keys are explicit, nonempty UTF-8 strings of
at most 1024 bytes without NUL; providers can impose additional restrictions.

The filesystem backend uses hashed filenames with metadata and body in one
atomic file. Use a **dedicated directory**: this format is incompatible with
the previous filesystem backend. Object sizes exclude its metadata overhead.

## S3, MinIO and R2

Provide a configured AWS SDK v2 client. Credentials and environment handling
belong to the application; the library does not modify environment variables.
The bucket must already exist.

```go
import (
    "github.com/aws/aws-sdk-go-v2/aws"
    awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
    storage "github.com/tavocg/go-storage/v2"
    s3backend "github.com/tavocg/go-storage/v2/backends/s3"
)

// cfg is an aws.Config configured by the application.
client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
    // For MinIO use its endpoint and path-style addressing.
    o.BaseEndpoint = aws.String("http://127.0.0.1:9000")
    o.UsePathStyle = true
})
backend, err := s3backend.New(client, "uploads")
// Check err before using backend.
store, err := storage.New(ctx, backend, storage.Options{
    Scope: "minio:local:uploads",
    Limits: storage.Limits{MaxBytes: 10 << 30},
})
```

For AWS, normally omit the endpoint override. For R2, use the account's S3
endpoint and its region configuration. Configure HTTP clients, retry policies
and credentials on the SDK client. The core package imports only the standard
library; AWS dependencies are isolated to `backends/s3`.

## Limits and concurrency

- `MaxBytes: 0` disables the byte limit. Negative values are invalid.
- A zero `RateLimit` disables rate limiting. Otherwise Operations and Interval
  must both be positive. This is a strict sliding window shared across Put,
  Get, Delete and List, with immediate rejection and no automatic waiting.
- Admitted operations count even when the key is missing, staging fails,
  capacity is exhausted or the backend returns an error. Invalid arguments
  rejected before admission do not count. Constructor scans, internal SDK
  retries, pagination and reading an already-open body do not add logical
  operations. These limits are therefore **not exact provider billing caps**.
- `errors.Is(err, storage.ErrRateLimited)` identifies a rate rejection;
  `errors.As` to `*storage.RateLimitError` gives `RetryAfter`.
- Byte accounting covers current logical object bodies, including existing
  objects discovered at initialization. It excludes staging files, metadata,
  multipart uploads and old versions. An initially over-quota namespace can
  be opened; deletions and non-growing replacements can recover capacity.
- Different keys may upload concurrently. A second mutation of a reserved key
  fails with `ErrObjectBusy`; the caller can retry later. Reads are allowed
  during writes and observe the backend's atomic publication semantics.
- Submissions reserve the positive difference from the previous object size.
  A shrinking overwrite or deletion releases bytes only after success.
- `WithSizeLimit(n)` rejects bodies larger than n; it never silently truncates.
  Zero permits empty bodies only; `-1` uses the default without a per-upload cap.

Put first admits an operation, then stages the body to a temporary disk file,
hashes it, and reserves its exact size before contacting the backend. Unknown
lengths and SDK retries work without buffering the whole object in memory.
`Options.TempDir` chooses the staging directory. Staging reads at most the
smaller of the configured quota and upload limit, plus a one-byte overrun probe.
Concurrent staging is not bounded by the byte quota; applications should bound
upload concurrency and provision temporary space. A reader blocked inside its
own Read must implement its own interruption mechanism.

## Coordination

Without `Options.Coordinator`, every Storage gets a **private in-memory
coordinator**. This safely coordinates concurrent goroutines of that instance
and initializes its accounting from existing objects. It provides no hidden
global state, cross-process synchronization or durable recovery.

For multiple instances in one process, explicitly share a coordinator:

```go
coordinator := storage.NewMemoryCoordinator()
options := storage.Options{
    Scope: "account:endpoint:uploads",
    Coordinator: coordinator,
    Limits: storage.Limits{
        MaxBytes: 10 << 30,
        Rate: storage.RateLimit{Operations: 100, Interval: time.Minute},
    },
}
first, err := storage.New(ctx, backend, options)
// Check err.
second, err := storage.New(ctx, backend, options)
```

For multiple processes, implement `storage.Coordinator` and `storage.Reservation`
using your database or coordination service. No Redis or database implementation
is imposed by this library. All access to one physical namespace must share
its scope, limits and coordinator. Independent instances without coordination,
external writers or a coordinator that expires live reservations invalidate
quota guarantees. Restarting an in-memory coordinator is not recovery for an
uncertain remote write.

See [the coordinator contract](docs/coordination.md) for atomicity, initialization
and recovery requirements, and [testing](docs/testing.md) for verified scenarios
and the reproducible MinIO fixture.
