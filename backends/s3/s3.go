// Package s3 implements the S3 storage.Backend
package s3

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/tavocg/go-storage"
)

type Options struct {
	//                           KEY,default
	Bucket                     string `env:"AWS_BUCKET"`
	AccessKeyID                string `env:"AWS_ACCESS_KEY_ID"`
	SecretAccessKey            string `env:"AWS_SECRET_ACCESS_KEY"`
	Region                     string `env:"AWS_REGION,auto"`
	Endpoint                   string `env:"AWS_ENDPOINT_URL_S3"`
	PublicEndpointURL          string `env:"AWS_PUBLIC_ENDPOINT_URL_S3"`
	RequestChecksumCalculation aws.RequestChecksumCalculation
	MaxSize                    int64 `env:"STORAGE_MAX_SIZE,10737418240"` // 1GB
}

func WithBucket(bucket string) func(*Options) {
	return func(o *Options) {
		o.Bucket = bucket
	}
}

func WithAccessKey(accessKeyID string) func(*Options) {
	return func(o *Options) {
		o.AccessKeyID = accessKeyID
	}
}

func WithSecretKey(secretKey string) func(*Options) {
	return func(o *Options) {
		o.SecretAccessKey = secretKey
	}
}

func WithRegion(region string) func(*Options) {
	return func(o *Options) {
		o.Region = region
	}
}

func WithEndpoint(endpoint string) func(*Options) {
	return func(o *Options) {
		o.Endpoint = endpoint
	}
}

func WithPublicEndpointURL(publicEndpointURL string) func(*Options) {
	return func(o *Options) {
		o.PublicEndpointURL = publicEndpointURL
	}
}

func WithRequestChecksumCalculation(value aws.RequestChecksumCalculation) func(*Options) {
	return func(o *Options) {
		o.RequestChecksumCalculation = value
	}
}

func WithMaxSize(maxSize int64) func(*Options) {
	return func(o *Options) {
		o.MaxSize = maxSize
	}
}

func New(ctx context.Context, optFuncs ...func(*Options)) (*storage.Storage, error) {
	opts := Options{}
	for _, optFunc := range optFuncs {
		optFunc(&opts)
	}

	if err := resolveTaggedOptions(&opts); err != nil {
		return nil, err
	}

	loadOpts, err := buildLoadOptions(opts)
	if err != nil {
		return nil, err
	}

	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if strings.TrimSpace(opts.Endpoint) != "" {
			o.UsePathStyle = true
		}
	})

	store := storage.New(client, opts.Bucket, opts.MaxSize, storage.WithPublicEndpointURL(opts.PublicEndpointURL))
	if err := store.LoadState(ctx); err != nil {
		return nil, fmt.Errorf("load storage state: %w", err)
	}

	return store, nil
}

func buildLoadOptions(opts Options) ([]func(*config.LoadOptions) error, error) {
	loadOpts := make([]func(*config.LoadOptions) error, 0, 2)
	if endpoint := strings.TrimSpace(opts.Endpoint); endpoint != "" {
		loadOpts = append(loadOpts, config.WithBaseEndpoint(endpoint))
	}

	if opts.RequestChecksumCalculation != aws.RequestChecksumCalculationUnset {
		loadOpts = append(loadOpts, config.WithRequestChecksumCalculation(opts.RequestChecksumCalculation))
	} else if strings.TrimSpace(opts.Endpoint) != "" {
		// Some S3-compatible providers reject the SDK's opportunistic CRC32
		// request checksums on streaming uploads.
		loadOpts = append(loadOpts, config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired))
	}

	return loadOpts, nil
}

func resolveTaggedOptions(opts *Options) error {
	value := reflect.ValueOf(opts)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return fmt.Errorf("options must be a non-nil pointer")
	}

	elem := value.Elem()
	if elem.Kind() != reflect.Struct {
		return fmt.Errorf("options must point to a struct")
	}

	elemType := elem.Type()
	for i := range elem.NumField() {
		field := elem.Field(i)
		fieldType := elemType.Field(i)
		tag := fieldType.Tag.Get("env")
		if tag == "" {
			continue
		}
		if !field.CanSet() {
			return fmt.Errorf("field %s must be settable", fieldType.Name)
		}

		envName, defaultValue := parseEnvTag(tag)
		if envName == "" {
			return fmt.Errorf("field %s has an invalid env tag", fieldType.Name)
		}

		if err := resolveTaggedField(field, fieldType.Name, envName, defaultValue); err != nil {
			return err
		}
	}

	return nil
}

func resolveTaggedField(field reflect.Value, fieldName, envName, defaultValue string) error {
	if currentValue, ok := currentFieldValue(field); ok {
		return setEnvIfNeeded(envName, currentValue)
	}

	if envValue, ok := os.LookupEnv(envName); ok && envValue != "" {
		if err := setFieldValue(field, envName, envValue); err != nil {
			return err
		}
		return nil
	}

	if defaultValue != "" {
		if err := setFieldValue(field, envName, defaultValue); err != nil {
			return err
		}
		return setEnvIfNeeded(envName, defaultValue)
	}

	return fmt.Errorf("%s is required for %s", envName, fieldName)
}

func parseEnvTag(tag string) (string, string) {
	parts := strings.SplitN(tag, ",", 2)
	envName := strings.TrimSpace(parts[0])
	if len(parts) == 1 {
		return envName, ""
	}

	return envName, strings.TrimSpace(parts[1])
}

func currentFieldValue(field reflect.Value) (string, bool) {
	switch field.Kind() {
	case reflect.String:
		value := field.String()
		return value, value != ""
	case reflect.Int64:
		value := field.Int()
		if value == 0 {
			return "", false
		}
		return strconv.FormatInt(value, 10), true
	default:
		return "", false
	}
}

func setFieldValue(field reflect.Value, envName, raw string) error {
	switch field.Kind() {
	case reflect.String:
		field.SetString(raw)
		return nil
	case reflect.Int64:
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid %s: %w", envName, err)
		}
		field.SetInt(value)
		return nil
	default:
		return fmt.Errorf("unsupported field kind %s", field.Kind())
	}
}

func setEnvIfNeeded(name, value string) error {
	current, ok := os.LookupEnv(name)
	if !ok || current != value {
		if err := os.Setenv(name, value); err != nil {
			return fmt.Errorf("set %s: %w", name, err)
		}
	}

	return nil
}
