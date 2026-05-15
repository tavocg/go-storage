/*
Copyright © 2026 NAME HERE <EMAIL ADDRESS>
*/

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	storage "github.com/tavocg/go-storage"
)

// putCmd represents the put command
var putCmd = &cobra.Command{
	Use:   "put SOURCE [KEY]",
	Short: "Upload an object to the configured backend",
	Long: `Upload SOURCE to the configured storage backend.

Use "-" as SOURCE to read from stdin. If KEY is omitted, the basename of
SOURCE is used when uploading a file; stdin uploads let the storage layer
generate a random key unless --key is provided.`,
	Args: cobra.RangeArgs(1, 2),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runPut(cmd.Context(), cmd, args)
	},
}

func init() {
	rootCmd.AddCommand(putCmd)

	flags := putCmd.Flags()
	flags.String("key", "", "object key to store under")
	flags.String("content-type", "", "content type to store with the object")
	flags.Int64("size", 0, "maximum number of bytes to read from SOURCE")
	flags.Bool("json", true, "print uploaded object metadata as JSON")

	mustBindFlag("put.key", putCmd, "key")
	mustBindFlag("put.content-type", putCmd, "content-type")
	mustBindFlag("put.size", putCmd, "size")
	mustBindFlag("put.json", putCmd, "json")
}

func runPut(ctx context.Context, cmd *cobra.Command, args []string) error {
	source := args[0]
	argKey := ""
	if len(args) == 2 {
		argKey = args[1]
	}

	flagKey := strings.TrimSpace(viper.GetString("put.key"))
	if argKey != "" && flagKey != "" {
		return fmt.Errorf("key provided both as argument and --key")
	}

	key := argKey
	if key == "" {
		key = flagKey
	}
	if key == "" && source != "-" {
		key = filepath.Base(source)
	}

	reader, size, contentType, closer, err := openPutSource(cmd, source)
	if err != nil {
		return err
	}
	if closer != nil {
		defer closer.Close()
	}

	if override := strings.TrimSpace(viper.GetString("put.content-type")); override != "" {
		contentType = override
	}

	if flag := cmd.Flags().Lookup("size"); flag != nil && flag.Changed {
		size = viper.GetInt64("put.size")
	}

	store, err := newStorageFromConfig(ctx)
	if err != nil {
		return err
	}

	putOpts := make([]storage.PutOption, 0, 3)
	if key != "" {
		putOpts = append(putOpts, storage.WithKey(key))
	}
	if contentType != "" {
		putOpts = append(putOpts, storage.WithContentType(contentType))
	}
	if size > 0 {
		putOpts = append(putOpts, storage.WithSizeLimit(size))
	}

	head, err := store.Put(ctx, reader, putOpts...)
	if err != nil {
		return err
	}

	if viper.GetBool("put.json") {
		return json.NewEncoder(cmd.OutOrStdout()).Encode(head)
	}

	_, err = fmt.Fprintln(cmd.OutOrStdout(), head.Key)
	return err
}

func openPutSource(cmd *cobra.Command, source string) (io.Reader, int64, string, io.Closer, error) {
	if source == "-" {
		return cmd.InOrStdin(), 0, "", nil, nil
	}

	file, err := os.Open(source)
	if err != nil {
		return nil, 0, "", nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, 0, "", nil, err
	}

	contentType := mime.TypeByExtension(filepath.Ext(source))
	return file, info.Size(), contentType, file, nil
}
