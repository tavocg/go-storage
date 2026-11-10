# go-storage

Go object-storage library with byte reservations and backend-specific operation
budgets. Includes filesystem and S3-compatible backends in one module, without a CLI.
Requires Go 1.26.3+.

The v3 API separates byte coordination from operation admission. Imports use
`github.com/tavocg/go-storage/v3`. Compared with v2, `storage.Limits.Rate` and
`storage.Coordinator.Allow` are removed: configure operation budgets when creating
the backend, using the separate `ratelimit.Coordinator` interface.

## Cloudflare R2 example

This configuration targets R2 **Standard** with margin below its included usage:

| Resource | Example limit | Maximum over 31 days |
| --- | --- | --- |
| Current object bodies | 9,000,000,000 bytes (9 decimal GB) | 9 GB continuously stored |
| Class A: uploads and each list page | 10,000 per 8 hours | 930,000 attempts |
| Class B: downloads | 100,000 per 8 hours | 9,300,000 attempts |

R2 Standard includes 10 GB-month, one million Class A and ten million Class B
requests per month. Delete requests are free. Source: [R2 pricing](https://developers.cloudflare.com/r2/pricing/).
The example uses decimal GB, not GiB. These allowances are shared by the account;
other buckets, clients, console activity and services also consume them.

```go
package example

import (
    "context"
    "fmt"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
    storage "github.com/tavocg/go-storage/v3"
    s3backend "github.com/tavocg/go-storage/v3/backends/s3"
    "github.com/tavocg/go-storage/v3/ratelimit"
)

func NewR2(
    ctx context.Context,
    accountID, bucket, accessKey, secretKey string,
    bytesCoordinator storage.Coordinator,
    requestsCoordinator ratelimit.Coordinator,
) (*storage.Storage, error) {
    client := awss3.New(awss3.Options{
        Region: "auto",
        BaseEndpoint: aws.String(fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountID)),
        UsePathStyle: true,
        RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
        Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
            return aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey}, nil
        }),
    })

    backend, err := s3backend.New(client, bucket,
        s3backend.WithRateLimits(ratelimit.Options{
            Scope: "r2:" + accountID, // Account-wide operation budgets.
            Coordinator: requestsCoordinator,
            Limits: map[ratelimit.Category]ratelimit.Limit{
                s3backend.ClassA: {Operations: 10_000, Interval: 8 * time.Hour},
                s3backend.ClassB: {Operations: 100_000, Interval: 8 * time.Hour},
            },
        }),
    )
    if err != nil { return nil, err }

    return storage.New(ctx, backend, storage.Options{
        Scope: "r2:" + accountID + ":" + bucket, // Bucket-specific byte accounting.
        Coordinator: bytesCoordinator,
        Limits: storage.Limits{MaxBytes: 9_000_000_000},
    })
}
```

The bucket must already exist. Pass shared **persistent** coordinators when
running multiple processes or preserving budgets across restarts. Nil selects
private in-memory coordination; it does not guarantee an account-wide monthly
budget. To share within one process, reuse `storage.NewMemoryCoordinator()` for
bytes and `&ratelimit.MemoryCoordinator{}` for operation budgets.

This example bounds attempts dispatched through these backends; it is not a
provider billing cap. Deduct other account usage from the available budgets.
If using multiple buckets, distribute the byte quota so their combined storage
fits the allowance. Versions, incomplete multipart uploads and provider metadata
are outside the logical byte quota. Other R2 storage classes have different
pricing and do not receive this Standard free tier.

## Reading, writing and deleting

```go
object, err := store.Put(ctx, "greetings/hello.txt", strings.NewReader("hello"),
    storage.WithContentType("text/plain"),
    storage.WithSizeLimit(1024),
)
// Check err.
head, body, err := store.Get(ctx, object.Key)
// Check err, use body and close it.
objects, err := store.List(ctx)
err = store.Delete(ctx, object.Key)
```

`Put` atomically replaces a key. `Get` returns `(Object, io.ReadCloser, error)`;
close its body. `List` returns all current objects, accumulating pages in memory.
`Delete` is idempotent. Keys are explicit, nonempty UTF-8 strings of at most
1024 bytes without NUL; providers may impose further restrictions.

## Operation budgets belong to backends

For the supported S3 operations, AWS and R2 use the same grouping:

| Category | Backend operations | R2 alias |
| --- | --- | --- |
| `s3.WriteRequests` | PutObject, each ListObjectsV2 page | `s3.ClassA` |
| `s3.ReadRequests` | GetObject | `s3.ClassB` |
| `s3.FreeRequests` | DeleteObject | Free; no limit unless configured |

Admission happens immediately before each SDK HTTP attempt. SDK retries, every
list page, and initialization scans consume their categories. There is no extra
charge at the Storage layer. A quota rejection or staging failure that never
calls the backend consumes no operation budget. Downloading from an already-open
body does not add requests. Failed admitted attempts are not refunded, even when
the provider might not bill them; this is deliberately conservative.

`ratelimit.Limit` uses a strict sliding window, not a continuously refilled token
bucket. Rejections return immediately. Check `errors.Is(err, ratelimit.ErrLimited)`
and `errors.As` to `*ratelimit.Error` for category, scope and `RetryAfter`.
Zero or missing category limits are unlimited; negative or incomplete limits are
invalid. The configuration is copied, so changing the caller's map has no effect.

The S3 backend requires an actual AWS SDK `*s3.Client` so per-request options are
honored. The configured SDK client's options are not mutated. Its default HTTP
client and standard `*http.Client` are supported; standard-client redirects are
also admitted. Opaque custom HTTP clients/transports must not hide additional
requests or retries behind one dispatch: use SDK retries, which are metered.
Transport-internal resends are outside this layer's visibility. Calls made
through the raw SDK client outside the backend do not consume these budgets.

If the initial mutation is rejected before dispatch, Storage releases its byte
reservation. If an earlier attempt was dispatched and a subsequent retry fails
admission, the mutation may already have happened: `PendingError` preserves the
reservation and wraps both `storage.ErrOutcomeUnknown` and the admission error.

## Filesystem

```go
backend, err := fs.New("./objects", fs.WithRateLimits(ratelimit.Options{
    Scope: "local:uploads",
    Limits: map[ratelimit.Category]ratelimit.Limit{
        fs.Operations: {Operations: 100, Interval: time.Minute},
    },
}))
// Check err; close the backend after all users stop.
store, err := storage.New(ctx, backend, storage.Options{
    Scope: "local:uploads",
    Limits: storage.Limits{MaxBytes: 9_000_000_000},
})
```

Filesystem counts each public backend Put, Get, Delete or List once, including
failed filesystem calls. Internal metadata reads are not additional operations.
The initial scan is a backend List and counts once. Use a dedicated directory;
objects use hashed filenames with metadata and body in one atomically renamed
file. This format is incompatible with the pre-v2 filesystem backend.

For AWS or MinIO, configure your SDK client normally and pass it to `s3.New`.
The library does not change environment variables. AWS prices depend on region
and storage class; the category names represent operations, not fixed prices.

## Byte quotas and recovery

`storage.Limits` contains only `MaxBytes`; zero disables it. Existing objects are
scanned on first initialization of the byte scope. Different keys can upload
concurrently. A pending mutation owns its key; another mutation fails with
`ErrObjectBusy`. Reads are allowed during writes.

Put stages and hashes the body in a temporary disk file, then reserves its exact
positive size increase. This supports unknown lengths and seekable SDK retries
without buffering whole objects in memory. `TempDir` chooses the staging directory.
`WithSizeLimit(n)` rejects oversized bodies without truncating or changing the
old object; zero permits only empty bodies and `-1` removes the per-upload bound.
Staging reads at most the smaller of that bound and the quota, plus one byte.
Concurrent staging space is not covered by the quota; bound upload concurrency
as appropriate. A reader blocked inside its own Read needs its own cancellation.

Byte reservations are released only after a known outcome. Normal finalization
uses a context independent of caller cancellation, bounded by `FinalizeTimeout`
(default five seconds). Unknown outcomes keep both bytes and mutation ownership.
Inspect `*storage.PendingError` and resolve only after excluding late publication.

The two coordinator interfaces are independent:

- `storage.Coordinator`: initialize byte state and reserve mutations; applications
  implement it for distributed byte quotas.
- `ratelimit.Coordinator`: atomically admit category-specific backend attempts;
  applications implement it for distributed request budgets.

With no coordinators supplied, each Storage and each backend respectively owns
private in-memory state. Multiple Storage instances sharing one backend share
its operation budget but still need to share their byte coordinator. Separate
backend instances need a shared request coordinator and account scope.

See [coordination](docs/coordination.md) and [testing](docs/testing.md).
