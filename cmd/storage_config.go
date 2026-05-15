package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	storage "github.com/tavocg/go-storage"
	s3backend "github.com/tavocg/go-storage/backends/s3"
)

const (
	defaultBackend   = "s3"
	defaultS3Region  = "auto"
	defaultS3MaxSize = int64(10 * 1024 * 1024 * 1024)
)

func initStorageConfig(cmd *cobra.Command) {
	viper.SetDefault("backend", defaultBackend)
	viper.SetDefault("s3.bucket", "")
	viper.SetDefault("s3.access-key-id", "")
	viper.SetDefault("s3.secret-access-key", "")
	viper.SetDefault("s3.region", defaultS3Region)
	viper.SetDefault("s3.max-size", defaultS3MaxSize)

	flags := cmd.PersistentFlags()
	flags.String("backend", defaultBackend, "storage backend to use")
	flags.String("s3-bucket", "", "S3 bucket name")
	flags.String("s3-access-key-id", "", "S3 access key ID")
	flags.String("s3-secret-access-key", "", "S3 secret access key")
	flags.String("s3-region", defaultS3Region, "S3 region")
	flags.Int64("s3-max-size", defaultS3MaxSize, "maximum total stored bytes")

	mustBindFlag("backend", cmd, "backend")
	mustBindFlag("s3.bucket", cmd, "s3-bucket")
	mustBindFlag("s3.access-key-id", cmd, "s3-access-key-id")
	mustBindFlag("s3.secret-access-key", cmd, "s3-secret-access-key")
	mustBindFlag("s3.region", cmd, "s3-region")
	mustBindFlag("s3.max-size", cmd, "s3-max-size")
}

func newStorageFromConfig(ctx context.Context) (*storage.Storage, error) {
	switch backend := strings.ToLower(strings.TrimSpace(viper.GetString("backend"))); backend {
	case "", "s3":
		return newS3Storage(ctx)
	default:
		return nil, fmt.Errorf("unsupported backend %q", backend)
	}
}

func newS3Storage(ctx context.Context) (*storage.Storage, error) {
	optFuncs := make([]func(*s3backend.Options), 0, 5)

	if value, ok := configuredString("s3.bucket", "s3-bucket", "STORAGE_S3_BUCKET"); ok {
		optFuncs = append(optFuncs, s3backend.WithBucket(value))
	}
	if value, ok := configuredString("s3.access-key-id", "s3-access-key-id", "STORAGE_S3_ACCESS_KEY_ID"); ok {
		optFuncs = append(optFuncs, s3backend.WithAccessKey(value))
	}
	if value, ok := configuredString("s3.secret-access-key", "s3-secret-access-key", "STORAGE_S3_SECRET_ACCESS_KEY"); ok {
		optFuncs = append(optFuncs, s3backend.WithSecretKey(value))
	}
	if value, ok := configuredString("s3.region", "s3-region", "STORAGE_S3_REGION"); ok {
		optFuncs = append(optFuncs, s3backend.WithRegion(value))
	}
	if value, ok := configuredInt64("s3.max-size", "s3-max-size", "STORAGE_S3_MAX_SIZE"); ok {
		optFuncs = append(optFuncs, s3backend.WithMaxSize(value))
	}

	return s3backend.New(ctx, optFuncs...)
}

func configuredString(key, flagName, envName string) (string, bool) {
	if !isConfigured(key, flagName, envName) {
		return "", false
	}

	return strings.TrimSpace(viper.GetString(key)), true
}

func configuredInt64(key, flagName, envName string) (int64, bool) {
	if !isConfigured(key, flagName, envName) {
		return 0, false
	}

	return viper.GetInt64(key), true
}

func isConfigured(key, flagName, envName string) bool {
	flag := rootCmd.PersistentFlags().Lookup(flagName)
	if flag != nil && flag.Changed {
		return true
	}

	if viper.InConfig(key) {
		return true
	}

	_, ok := os.LookupEnv(envName)
	return ok
}

func mustBindFlag(key string, cmd *cobra.Command, name string) {
	flag := cmd.PersistentFlags().Lookup(name)
	if flag == nil {
		flag = cmd.Flags().Lookup(name)
	}
	if flag == nil {
		panic("missing flag: " + name)
	}

	cobra.CheckErr(viper.BindPFlag(key, flag))
}
