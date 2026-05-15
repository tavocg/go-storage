/*
Copyright © 2026 NAME HERE <EMAIL ADDRESS>
*/

package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	storage "github.com/tavocg/go-storage"
)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get KEY [DEST]",
	Short: "Download an object from the configured backend",
	Long: `Fetch KEY from the configured storage backend.

If DEST is omitted or "-", the object body is written to stdout.`,
	Args: cobra.RangeArgs(1, 2),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runGet(cmd.Context(), cmd, args)
	},
}

func init() {
	rootCmd.AddCommand(getCmd)

	getCmd.Flags().StringP("output", "o", "", "destination file path, or - for stdout")
	mustBindFlag("get.output", getCmd, "output")
}

func runGet(ctx context.Context, cmd *cobra.Command, args []string) error {
	dest := ""
	if len(args) == 2 {
		dest = args[1]
	}

	flagDest := strings.TrimSpace(viper.GetString("get.output"))
	if dest != "" && flagDest != "" {
		return fmt.Errorf("destination provided both as argument and --output")
	}
	if dest == "" {
		dest = flagDest
	}

	store, err := newStorageFromConfig(ctx)
	if err != nil {
		return err
	}

	body, err := store.Get(ctx, &storage.ObjectHead{Key: args[0]})
	if err != nil {
		return err
	}
	defer body.Close()

	writer, closer, err := openGetDestination(cmd, dest)
	if err != nil {
		return err
	}
	if closer != nil {
		defer closer.Close()
	}

	_, err = io.Copy(writer, body)
	return err
}

func openGetDestination(cmd *cobra.Command, dest string) (io.Writer, io.Closer, error) {
	if dest == "" || dest == "-" {
		return cmd.OutOrStdout(), nil, nil
	}

	dir := filepath.Dir(dest)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, nil, err
		}
	}

	file, err := os.Create(dest)
	if err != nil {
		return nil, nil, err
	}

	return file, file, nil
}
