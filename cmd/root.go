/*
Copyright © 2026 Gustavo Calvo <tavo@tavo.cr>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package cmd

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "store",
	Short: "Store, fetch, and delete objects through the configured backend",
	Long: `store is a thin CLI for the storage package in this repository.

Backend settings can come from a config file, environment variables, or flags.
The same backend configuration is shared by the put, get, and delete commands.`,
	SilenceUsage: true,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file path (defaults to store.yaml in standard config directories)")
	initStorageConfig(rootCmd)
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		for _, d := range findConfigDirs() {
			viper.AddConfigPath(d)
		}

		viper.SetConfigType("yaml")
		viper.SetConfigName("store")
	}

	viper.SetEnvPrefix("STORE")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	var notFound viper.ConfigFileNotFoundError
	if err != nil && !errors.As(err, &notFound) {
		cobra.CheckErr(err)
	}
}

func findConfigDirs() []string {
	dirs := []string{}

	if wd, err := os.Getwd(); err == nil && wd != "" {
		dirs = append(dirs, wd)
	}

	if xdgCfgDir := os.Getenv("XDG_CONFIG_HOME"); xdgCfgDir != "" {
		if !slices.Contains(dirs, xdgCfgDir) {
			dirs = append(dirs, xdgCfgDir)
		}
	}

	if home, err := os.UserHomeDir(); err == nil && home != "" {
		defCfgDir := filepath.Join(home, ".config")
		if !slices.Contains(dirs, defCfgDir) {
			dirs = append(dirs, defCfgDir)
		}
	}

	dirs = append(dirs, "/usr/local/etc", "/etc")

	return dirs
}
