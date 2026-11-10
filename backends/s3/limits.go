package s3

import (
	"fmt"
	"net/http"
	"sync/atomic"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/tavocg/go-storage/v3/ratelimit"
)

const (
	// WriteRequests includes PutObject and every ListObjectsV2 page.
	WriteRequests ratelimit.Category = "s3:write"
	// ReadRequests includes GetObject.
	ReadRequests ratelimit.Category = "s3:read"
	// FreeRequests includes DeleteObject. It is unlimited unless configured.
	FreeRequests ratelimit.Category = "s3:free"
	// ClassA and ClassB name the same groups using Cloudflare R2 terminology.
	ClassA = WriteRequests
	ClassB = ReadRequests
)

type options struct{ rates ratelimit.Options }
type Option func(*options)

// WithRateLimits configures budgets at the backend's HTTP boundary. Scope can
// identify an entire account so that several buckets share the same budgets.
func WithRateLimits(rates ratelimit.Options) Option { return func(o *options) { o.rates = rates } }

func newLimiter(opts []Option) (*ratelimit.Limiter, error) {
	var config options
	for _, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("s3: nil option")
		}
		opt(&config)
	}
	for category := range config.rates.Limits {
		if category != WriteRequests && category != ReadRequests && category != FreeRequests {
			return nil, fmt.Errorf("s3: unknown operation category %q", category)
		}
	}
	return ratelimit.New(config.rates)
}

// A tracker belongs to one SDK invocation, including all its retries. A
// rejected initial attempt is known unchanged; a rejected retry may follow an
// already-published write, and must keep its byte reservation.
type requestTracker struct {
	limiter  *ratelimit.Limiter
	category ratelimit.Category
	sent     atomic.Bool
}

func (t *requestTracker) options(o *awss3.Options) {
	client := o.HTTPClient
	if client == nil {
		client = awshttp.NewBuildableClient()
	}
	if standard, ok := client.(*http.Client); ok {
		// Instrument RoundTrip rather than Do so that redirects are admitted too.
		clone := *standard
		transport := clone.Transport
		if transport == nil {
			transport = http.DefaultTransport
		}
		clone.Transport = &meteredTransport{next: transport, tracker: t}
		o.HTTPClient = &clone
	} else {
		// The SDK's default BuildableClient does not follow redirects. Opaque custom
		// clients must not hide additional retries; use the SDK retry mechanism.
		o.HTTPClient = &meteredClient{next: client, tracker: t}
	}
}

func (t *requestTracker) admit(request *http.Request) error {
	if err := t.limiter.Allow(request.Context(), t.category); err != nil {
		if request.Body != nil {
			request.Body.Close()
		}
		return &admissionError{err: err}
	}
	t.sent.Store(true)
	return nil
}

type admissionError struct{ err error }

func (e *admissionError) Error() string        { return e.err.Error() }
func (e *admissionError) Unwrap() error        { return e.err }
func (e *admissionError) RetryableError() bool { return false }

type meteredClient struct {
	next    awss3.HTTPClient
	tracker *requestTracker
}

func (c *meteredClient) Do(request *http.Request) (*http.Response, error) {
	if err := c.tracker.admit(request); err != nil {
		return nil, err
	}
	return c.next.Do(request)
}

type meteredTransport struct {
	next    http.RoundTripper
	tracker *requestTracker
}

func (c *meteredTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := c.tracker.admit(request); err != nil {
		return nil, err
	}
	return c.next.RoundTrip(request)
}
