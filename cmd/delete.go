/*
Copyright © 2026 NAME HERE <EMAIL ADDRESS>
*/

package cmd

import (
	"context"

	"github.com/spf13/cobra"

	storage "github.com/tavocg/go-storage"
)

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "delete KEY",
	Short: "Delete an object from the configured backend",
	Long:  `Delete KEY from the configured storage backend.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runDelete(cmd.Context(), args[0])
	},
}

func init() {
	rootCmd.AddCommand(deleteCmd)
}

func runDelete(ctx context.Context, key string) error {
	store, err := newStorageFromConfig(ctx)
	if err != nil {
		return err
	}

	return store.Delete(ctx, &storage.ObjectHead{Key: key})
}
